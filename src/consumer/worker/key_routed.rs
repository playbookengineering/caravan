use std::{
    collections::HashMap,
    fmt::Debug,
    time::{Duration, Instant},
};

use crate::{codec::Codec, consumer::{IncomingMessage, MessageBus}};

use super::{launch_worker, WorkerContext, WorkerId, WorkerState};

#[derive(Debug, Clone)]
pub struct KeyRoutedPoolConfig {
    pub inactivity_duration: Duration,
    pub queue_size: usize,
}

pub struct KeyRouted<B: MessageBus, C: Codec> {
    workers: HashMap<WorkerId, WorkerState<B>>,
    fallback: WorkerState<B>,
    context: WorkerContext<B, C>,
    cfg: KeyRoutedPoolConfig,
}

impl<B: MessageBus, C: Codec> KeyRouted<B, C> {
    pub fn new(cfg: KeyRoutedPoolConfig, context: WorkerContext<B, C>) -> Self {
        let fallback = launch_worker(
            context.clone(),
            WorkerId("fallback".to_owned()),
            cfg.queue_size,
        );

        Self {
            workers: Default::default(),
            fallback,
            context,
            cfg,
        }
    }

    pub async fn dispatch(&mut self, msg: B::IncomingMessage) {
        match msg.key() {
            Some(key) => {
                let worker_id = std::str::from_utf8(key)
                    .map(ToString::to_string)
                    .map(WorkerId)
                    .unwrap_or_else(|_| WorkerId(hex::encode(key)));

                let worker = self.workers.entry(worker_id.clone()).or_insert_with(|| {
                    launch_worker(self.context.clone(), worker_id, self.cfg.queue_size)
                });

                worker.dispatch(msg).await
            }
            None => self.fallback.dispatch(msg).await,
        }
    }

    pub fn do_cleanup(&mut self, now: Instant) {
        let limit = self.cfg.inactivity_duration;

        let to_remove = self
            .workers
            .iter()
            .filter_map(|(key, worker)| {
                let elapsed = now.duration_since(worker.last_received);

                (elapsed > limit).then(|| key.clone())
            })
            .collect::<Vec<_>>();

        for key in to_remove {
            if let Some(worker) = self.workers.remove(&key) {
                // dispose without further blocking
                tokio::spawn(async move { worker.dispose().await });
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::codec::Json;
    use crate::consumer::router::Router;
    use crate::consumer::task_local::TaskLocal;
    use crate::consumer::worker::{Flavour, WorkerPool};
    use crate::consumer::{Extension, Extensions};
    use crate::message::{RawHeaders, RawMessage};
    use crate::test::{TestIncomingMessage, TestMessage, TestMessageBus};

    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    fn kr_config_default() -> KeyRoutedPoolConfig {
        KeyRoutedPoolConfig {
            inactivity_duration: Duration::from_secs(10),
            queue_size: 128,
        }
    }

    fn kr_config(duration: Duration) -> KeyRoutedPoolConfig {
        KeyRoutedPoolConfig {
            inactivity_duration: duration,
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
    async fn key_routed_pool_dispatch() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config_default(), context);

        for i in 0..100 {
            let message = TestMessage::new(0);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming).await;
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test0");
            assert_eq!(call_counter, i);

            let message = TestMessage::new(1);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming).await;
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test1");
            assert_eq!(call_counter, i);

            let message = TestMessage::new(2);

            let incoming = TestIncomingMessage::create_raw_json(message.clone());
            workers.dispatch(incoming).await;
            let (processed, worker_id, call_counter) = rx.recv().await.unwrap();

            assert_eq!(message, processed);
            assert_eq!(worker_id.get(), "test2");
            assert_eq!(call_counter, i);
        }
    }

    #[tokio::test]
    async fn key_routed_pool_delete_inactive_workers() {
        let (tx, mut _rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config(dur), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming).await;
        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        match workers.pool {
            Flavour::KeyRouted(kr) => assert!(
                kr.workers.is_empty(),
                "workers count: {}, expected empty",
                kr.workers.len()
            ),
            _ => unreachable!("key routed pool is used"),
        }
    }

    #[tokio::test]
    async fn key_routed_pool_recreate_workers() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config(dur), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming.clone()).await;
        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        workers.dispatch(incoming.clone()).await;

        match workers.pool {
            Flavour::KeyRouted(kr) => {
                assert_eq!(kr.workers.len(), 1, "expected worker to be recreated")
            }
            _ => unreachable!("key routed pool is used"),
        };

        assert_eq!(rx.recv().await.unwrap().2, 0);
        assert_eq!(
            rx.recv().await.unwrap().2,
            0,
            "previous worker was not removed (old TLS)"
        );
    }

    #[tokio::test]
    async fn key_routed_pool_fallback() {
        let (tx, mut rx) = unbounded_channel::<(RawMessage, WorkerId)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().fallback_handler(fallback_handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);

        let mut workers = WorkerPool::<TestMessageBus, _>::key_routed(kr_config_default(), context);

        let incoming = TestIncomingMessage {
            key: None,
            headers: Default::default(),
            payload: vec![42],
        };

        workers.dispatch(incoming).await;
        let (processed_raw, worker_id) = rx.recv().await.unwrap();

        assert_eq!(processed_raw.payload, [42]);
        assert_eq!(processed_raw.headers, RawHeaders::default());
        assert_eq!(worker_id.get(), "fallback");
    }
}