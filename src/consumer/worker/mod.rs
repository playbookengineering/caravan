use futures_lite::FutureExt;
use std::{
    fmt::Debug,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{instrument, Instrument, Span};

use crate::{codec::Codec, consumer::Confirmation};
use super::{
    context::{LocalCache, ProcessContext}, extension::Extensions, router::Router, task_local::{TaskLocal, TASK_LOCALS}, Hooks, IncomingMessage, MessageBus
};

mod fixed;
mod key_routed;
mod parition_routed;

pub use fixed::*;
pub use key_routed::*;
pub use parition_routed::*;

const DEFAULT_QUEUE_SIZE: usize = 128;

pub struct WorkerPool<B: MessageBus, C: Codec> {
    pool: Flavour<B, C>,
}

pub struct WorkerContext<B: MessageBus, C: Codec> {
    extensions: Arc<Extensions>,
    router: Arc<Router<B, C>>,
    hooks: Arc<Hooks<B, C>>,
    codec: Arc<C>,
}

impl<B: MessageBus, C: Codec> Clone for WorkerContext<B, C> {
    fn clone(&self) -> Self {
        Self {
            extensions: Arc::clone(&self.extensions),
            router: Arc::clone(&self.router),
            hooks: Arc::clone(&self.hooks),
            codec: Arc::clone(&self.codec),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WorkerPoolConfig {
    Fixed(FixedPoolConfig),
    KeyRouted(KeyRoutedPoolConfig),
    PartitionedRouted(PartitionRoutedPoolConfig),
}

impl WorkerPoolConfig {
    pub fn fixed(count: usize) -> Self {
        Self::Fixed(FixedPoolConfig::new(count))
    }

    pub fn key_routed(inactivity_duration: Duration) -> Self {
        Self::KeyRouted(KeyRoutedPoolConfig {
            inactivity_duration,
            queue_size: DEFAULT_QUEUE_SIZE,
        })
    }

    pub(crate) fn timer(&self) -> Option<tokio::time::Interval> {
        match self {
            WorkerPoolConfig::Fixed(_) => None,
            WorkerPoolConfig::PartitionedRouted(pr_config) => {
                let duration = pr_config.inactivity_duration;

                Some(tokio::time::interval_at(
                    tokio::time::Instant::now() + duration,
                    duration,
                ))
            }
            WorkerPoolConfig::KeyRouted(kr_config) => {
                let duration = kr_config.inactivity_duration;

                Some(tokio::time::interval_at(
                    tokio::time::Instant::now() + duration,
                    duration,
                ))
            }
        }
    }
}

enum Flavour<B: MessageBus, C: Codec> {
    Fixed(Fixed<B>),
    KeyRouted(KeyRouted<B, C>),
    PartitionRouted(PartitionRouted<B, C>),
}

impl<B: MessageBus, C: Codec> WorkerContext<B, C> {
    pub fn new(router: Router<B, C>, extensions: Extensions, hooks: Hooks<B, C>, codec: C) -> Self {
        Self {
            extensions: Arc::new(extensions),
            router: Arc::new(router),
            hooks: Arc::new(hooks),
            codec: Arc::new(codec),
        }
    }

    pub fn router(&self) -> &Router<B, C> {
        &self.router
    }

    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    pub fn hooks(&self) -> &Hooks<B, C> {
        &self.hooks
    }
}

impl<B: MessageBus, C: Codec> WorkerPool<B, C> {
    pub fn new(config: WorkerPoolConfig, context: WorkerContext<B, C>) -> Self {
        let worker = match config.clone() {
            WorkerPoolConfig::Fixed(cfg) => Self::fixed(cfg, context),
            WorkerPoolConfig::KeyRouted(cfg) => Self::key_routed(cfg, context),
            WorkerPoolConfig::PartitionedRouted(cfg) => Self::partition_routed(cfg, context),
        };

        tracing::info!("Initialized worker with config: {config:?}");

        worker
    }

    fn fixed(cfg: FixedPoolConfig, context: WorkerContext<B, C>) -> Self {
        Self {
            pool: Flavour::Fixed(Fixed::new(cfg, context)),
        }
    }

    fn key_routed(cfg: KeyRoutedPoolConfig, context: WorkerContext<B, C>) -> Self {
        Self {
            pool: Flavour::KeyRouted(KeyRouted::new(cfg, context)),
        }
    }
    fn partition_routed(cfg: PartitionRoutedPoolConfig, context: WorkerContext<B, C>) -> Self {
        Self {
            pool: Flavour::PartitionRouted(PartitionRouted::new(cfg, context)),
        }
    }

    pub async fn dispatch(&mut self, message: B::IncomingMessage) {
        match &mut self.pool {
            Flavour::Fixed(f) => f.dispatch(message).await,
            Flavour::KeyRouted(kr) => kr.dispatch(message).await,
            Flavour::PartitionRouted(pr) => pr.dispatch(message).await,
        }
    }

    pub fn do_cleanup(&mut self, now: Instant) {
        match &mut self.pool {
            Flavour::KeyRouted(pool) => pool.do_cleanup(now),
            Flavour::PartitionRouted(pool) => pool.do_cleanup(now),
            _ => {}
        }
    }

    #[cfg(test)]
    fn set_stable_seed(&mut self) {
        match &mut self.pool {
            Flavour::Fixed(f) => f.set_stable_seed(),
            Flavour::PartitionRouted(f) => f.set_stable_seed(),
            Flavour::KeyRouted(_) => unimplemented!(),
        }
    }
}

pub struct WorkerState<B: MessageBus> {
    sender: Sender<WorkerEvent<B>>,
    last_received: Instant,
}

impl<B: MessageBus> WorkerState<B> {
    async fn dispatch(&mut self, message: B::IncomingMessage) {
        self.last_received = Instant::now();

        self.sender
            .send(WorkerEvent::IncomingMessage(message))
            .await
            .expect("failed to send, worker receiver should be alive");
    }

    async fn dispose(self) {
        self.sender
            .send(WorkerEvent::Termination)
            .await
            .expect("failed to send, worker receiver should be alive");
    }
}

enum WorkerEvent<B: MessageBus> {
    IncomingMessage(B::IncomingMessage),
    Termination,
}

impl<B: MessageBus> Debug for WorkerEvent<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerEvent::IncomingMessage(m) => f
                .debug_struct("WorkerEvent::IncomingMessage")
                .field("payload_len", &m.payload().len())
                .finish(),
            WorkerEvent::Termination => f.debug_struct("WorkerEvent::Termination").finish(),
        }
    }
}

fn launch_worker<B: MessageBus, C: Codec>(
    context: WorkerContext<B, C>,
    id: WorkerId,
    queue_size: usize,
) -> WorkerState<B> {
    let (tx, rx) = mpsc::channel(queue_size);

    tokio::spawn(TASK_LOCALS.scope(Default::default(), worker::<B, C>(context, rx, id)));

    WorkerState {
        sender: tx,
        last_received: Instant::now(),
    }
}

#[instrument(skip_all, fields(id = id.0))]
async fn worker<B: MessageBus, C: Codec>(
    worker_context: WorkerContext<B, C>,
    mut receiver: Receiver<WorkerEvent<B>>,
    id: WorkerId,
) {
    TaskLocal::<WorkerId>::set_internal(id);

    tracing::info!("Start listening");

    while let Some(event) = receiver.recv().await {
        tracing::debug!("Received event: {event:?}");

        let message = Arc::new(match event {
            WorkerEvent::IncomingMessage(m) => m,
            WorkerEvent::Termination => return,
        });

        let extensions = worker_context.extensions();
        let router = worker_context.router();

        let span = message.make_span();
        let mut cache = LocalCache::default();

        if cfg!(feature = "opentelemetry") {
            extract_otel_context(&span, message.headers());
        }

        async {
            let mut process_context = ProcessContext::new(
                message.clone(),
                extensions,
                &mut cache,
                &*worker_context.codec,
            );

            if let Some(kind) = process_context.kind() {
                Span::current().record("convoy.kind", kind);
            }

            tracing::info!("Message: begin processing");

            worker_context
                .hooks()
                .before_processing(&mut process_context);

            let confirmation = match AssertUnwindSafe(router.route(&process_context))
                .catch_unwind()
                .await
            {
                Ok(Ok(confirmation)) => confirmation,
                Ok(Err(err)) => {
                    tracing::error!("Handler error occurred: {err}");
                    Confirmation::Reject
                }
                Err(err) => {
                    tracing::error!("Handler panicked with error: {err:?}");
                    Confirmation::Reject
                }
            };

            let confirmation_store_result = match confirmation {
                Confirmation::Ack => message.ack().await,
                Confirmation::Nack => message.nack().await,
                Confirmation::Reject => message.reject().await,
            };

            if let Err(err) = confirmation_store_result {
                tracing::error!("Failed to store confirmation result: {err}");
            }

            worker_context
                .hooks()
                .after_processing(&process_context, confirmation);

            tracing::info!(
                "Message {} processed, confirmation: {}",
                process_context.kind().unwrap_or("unknown"),
                confirmation,
            );
        }
        .instrument(span)
        .await;
    }

    tracing::info!("Stop listening");
}

#[cfg(not(feature = "opentelemetry"))]
#[allow(unused)]
#[inline(always)]
fn extract_otel_context(_: &tracing::Span, _: &crate::message::RawHeaders) {}

#[cfg(feature = "opentelemetry")]
#[inline(always)]
fn extract_otel_context(span: &tracing::Span, headers: &crate::message::RawHeaders) {
    use opentelemetry::global::get_text_map_propagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let parent_context = get_text_map_propagator(|propagator| propagator.extract(headers));
    span.set_parent(parent_context);
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct WorkerId(String);

impl WorkerId {
    #[allow(unused)]
    fn get(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::codec::Json;
    use crate::consumer::Extension;
    use crate::consumer::{Confirmation, Hook};
    use crate::test::{TestIncomingMessage, TestMessage, TestMessageBus};

    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    async fn panicking_handler(
        message: TestMessage,
        sender: Extension<UnboundedSender<(TestMessage, WorkerId, usize)>>,
        worker_id: TaskLocal<WorkerId>,
        mut call_counter: TaskLocal<usize>,
    ) {
        let worker_id = worker_id.with(|x| x.clone());
        let counter = call_counter.get();
        call_counter.set(counter + 1);

        if counter == 0 {
            panic!("Handler panicked");
        } else {
            sender.send((message, worker_id, counter)).unwrap();
        }
    }

    fn fixed_config_default() -> FixedPoolConfig {
        FixedPoolConfig {
            count: 3,
            queue_size: 128,
        }
    }

    #[tokio::test]
    async fn panicking_handlers_are_not_crashing_worker() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let router = Router::<TestMessageBus, _>::default().message_handler(panicking_handler);
        let extensions = Extensions::default().insert(tx);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::fixed(fixed_config_default(), context);
        workers.set_stable_seed();

        let message = TestMessage::new(0);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming.clone()).await;
        workers.dispatch(incoming).await;

        let (_, _, count) = rx.recv().await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn hooks_are_executed() {
        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            ProcessStart,
            ProcessEnd(Confirmation),
        }

        struct TestHook(UnboundedSender<Event>);

        impl<B: MessageBus, C: Codec> Hook<B, C> for TestHook {
            fn before_processing(&self, _: &mut ProcessContext<'_, B, C>) {
                self.0.send(Event::ProcessStart).unwrap();
            }

            fn after_processing(&self, _: &ProcessContext<'_, B, C>, confirmation: Confirmation) {
                self.0.send(Event::ProcessEnd(confirmation)).unwrap();
            }
        }

        let (tx, mut rx) = unbounded_channel();

        let hooks = Hooks::default().push(TestHook(tx));

        let context = WorkerContext::new(Router::default(), Extensions::default(), hooks, Json);

        let mut workers =
            WorkerPool::<TestMessageBus, Json>::fixed(fixed_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;

        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();

        assert_eq!(event1, Event::ProcessStart);
        assert_eq!(event2, Event::ProcessEnd(Confirmation::Reject));
    }

    #[tokio::test]
    async fn hooks_with_cache() {
        struct TestHook(UnboundedSender<Option<Num>>);

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        struct Num(i32);

        impl<B: MessageBus, C: Codec> Hook<B, C> for TestHook {
            fn before_processing(&self, req: &mut ProcessContext<'_, B, C>) {
                req.cache_mut().set(Num(42));
            }

            fn after_processing(&self, req: &ProcessContext<'_, B, C>, _: Confirmation) {
                let num: Option<Num> = req.cache().get().cloned();
                self.0.send(num).unwrap();
            }
        }

        let (tx, mut rx) = unbounded_channel();
        let hooks = Hooks::default().push(TestHook(tx));

        let context = WorkerContext::new(Router::default(), Extensions::default(), hooks, Json);
        let mut workers = WorkerPool::<TestMessageBus, _>::fixed(fixed_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;

        let num = rx.recv().await.unwrap();

        assert_eq!(num, Some(Num(42)));
    }
}
