use rand::{thread_rng, Rng};
use std::fmt::Debug;

use crate::{codec::Codec, consumer::{worker::{launch_worker, WorkerId}, IncomingMessage, MessageBus}};

use super::{WorkerContext, WorkerState};

const DEFAULT_QUEUE_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub struct FixedPoolConfig {
    pub count: usize,
    pub queue_size: usize,
}

impl FixedPoolConfig {
    pub fn new(workers_count: usize) -> Self {
        Self {
            count: workers_count,
            queue_size: DEFAULT_QUEUE_SIZE,
        }
    }

    pub fn queue_size(self, size: usize) -> Self {
        Self {
            queue_size: size,
            ..self
        }
    }
}

pub(crate) struct Fixed<B: MessageBus> {
    pub workers: Vec<WorkerState<B>>,
    hasher: ahash::RandomState,
}

impl<B: MessageBus> Fixed<B> {
    pub fn new<C: Codec>(config: FixedPoolConfig, context: WorkerContext<B, C>) -> Self {
        let FixedPoolConfig { count, queue_size } = config;
        assert!(count > 0, "Count must be greater than zero!");

        let hasher = ahash::RandomState::default();
        let workers = (0..count)
            .map(|idx| {
                launch_worker::<B, C>(context.clone(), WorkerId(idx.to_string()), queue_size)
            })
            .collect();

        Self { workers, hasher }
    }

    #[cfg(test)]
    pub fn set_stable_seed(&mut self) {
        self.hasher = ahash::RandomState::with_seeds(0x3038, 0x3039, 0x9394, 0x1234);
    }

    pub async fn dispatch(&mut self, msg: B::IncomingMessage) {
        let worker_idx = match msg.key() {
            Some(key) => {
                tracing::info!("DISPACZING, message key: {}", std::str::from_utf8(key).unwrap());
                let hash = self.hasher.hash_one(key) as usize;
                hash % self.workers.len()
            }
            None => {
                tracing::info!("message does not contain a key, fallback to rand");
                thread_rng().gen_range(0..self.workers.len())
            }
        };

        self.workers[worker_idx].dispatch(msg).await
    }
}



#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::codec::Json;
    use crate::consumer::router::Router;
    use crate::consumer::task_local::TaskLocal;
    use crate::consumer::worker::{Flavour, WorkerContext, WorkerId, WorkerPool};
    use crate::consumer::{Extension, Extensions};
    use crate::message::{RawHeaders, RawMessage};
    use crate::test::{TestIncomingMessage, TestMessage, TestMessageBus};

    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    fn fixed_config_default() -> FixedPoolConfig {
        FixedPoolConfig {
            count: 3,
            queue_size: 128,
        }
    }


    fn fixed_config(count: usize) -> FixedPoolConfig {
        FixedPoolConfig {
            count,
            queue_size: 128,
        }
    }
   
    async fn handler(
        message: TestMessage,
        sender: Extension<UnboundedSender<(TestMessage, WorkerId, usize)>>,
        worker_id: TaskLocal<WorkerId>,
        mut call_counter: TaskLocal<usize>,
    ) {
        let worker_id = worker_id.with(|x| x.clone());
        let counter = call_counter.get();

        sender.send((message, worker_id, counter)).unwrap();

        call_counter.set(counter + 1);
    }

    async fn fallback_handler(
        message: RawMessage,
        sender: Extension<UnboundedSender<(RawMessage, WorkerId)>>,
        worker_id: TaskLocal<WorkerId>,
    ) {
        let worker_id = worker_id.with(|x| x.clone());
        sender.send((message, worker_id)).unwrap();
    }

    #[tokio::test]
    #[ignore = "unstable test, is seed really stable"]
    async fn fixed_pool_dispatch() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let router = Router::<TestMessageBus, _>::default().message_handler(handler);
        let extensions = Extensions::default().insert(tx);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::fixed(fixed_config_default(), context);
        workers.set_stable_seed();

        let message = TestMessage::new(0);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());

        workers.dispatch(incoming).await;
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "1");

        let message = TestMessage::new(12);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "0");

        let message = TestMessage::new(9);

        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        let (processed, worker_id, _) = rx.recv().await.unwrap();

        assert_eq!(message, processed);
        assert_eq!(worker_id.get(), "2");
    }


    #[tokio::test]
    async fn fixed_pool_fallback() {
        let (tx, mut rx) = unbounded_channel::<(RawMessage, WorkerId)>();

        let router = Router::<TestMessageBus, _>::default().fallback_handler(fallback_handler);
        let extensions = Extensions::default().insert(tx);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::<TestMessageBus, _>::fixed(fixed_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;
        let (processed_raw, _) = rx.recv().await.unwrap();

        assert_eq!(processed_raw.payload, [42]);
        assert_eq!(processed_raw.headers, RawHeaders::default());
    }

    #[tokio::test]
    async fn fixed_worker_workers_are_not_cleaned_up() {
        let (tx, mut _rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::<_, Json>::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let count = 10;
        let mut workers = WorkerPool::<TestMessageBus, Json>::fixed(fixed_config(10), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        match workers.pool {
            Flavour::Fixed(f) => {
                assert_eq!(f.workers.len(), count);
            }
            _ => unreachable!("fixed pool is used"),
        }
    }
}