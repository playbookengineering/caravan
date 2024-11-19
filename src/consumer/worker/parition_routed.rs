use std::{
    collections::HashMap,
    fmt::Debug,
    time::{Duration, Instant},
};

use rand::{thread_rng, Rng};

use crate::{codec::Codec, consumer::{IncomingMessage, MessageBus}};

use super::{launch_worker, WorkerContext, WorkerId, WorkerState};

#[derive(Debug, Clone)]
pub struct PartitionRoutedPoolConfig {
    pub inactivity_duration: Duration,
    pub queue_size: usize,
}

pub struct PartitionRouted<B: MessageBus, C: Codec> {
    workers: HashMap<i32, HashMap<usize, WorkerState<B>>>,
    context: WorkerContext<B, C>,
    cfg: PartitionRoutedPoolConfig,
    hasher: ahash::RandomState,
}

impl<B: MessageBus, C: Codec> PartitionRouted<B, C> {
    pub fn new(cfg: PartitionRoutedPoolConfig, context: WorkerContext<B, C>) -> Self {
        let hasher = ahash::RandomState::default();

        Self {
            workers: Default::default(),
            context,
            cfg,
            hasher
        }
    }

    pub async fn dispatch(&mut self, msg: B::IncomingMessage) {
        let partition = msg.partition().unwrap_or(-1);

        // separate fixed size queue for each partition
        let partition_queue = self.workers.entry(partition).or_insert_with(|| {
           tracing::info!("parition_routed: creating empty queue for partition: {partition}");
           Default::default() 
        });

        // task index
        let idx = match msg.key() {
            Some(key) => { // calculate using key to preserve order
                let hash = self.hasher.hash_one(key) as usize;
                hash % self.cfg.queue_size
            }
            None => thread_rng().gen_range(0..self.cfg.queue_size) // we dont care about order of messages without key
        };

        let worker = partition_queue.entry(idx).or_insert_with(|| {
            launch_worker(self.context.clone(), WorkerId(format!("partition-{}-task-{}", partition, idx)), self.cfg.queue_size)
        });

        worker.dispatch(msg).await
    }

    pub fn do_cleanup(&mut self, now: Instant) {
        let limit = self.cfg.inactivity_duration;

        // cleanup partitions only
        let to_remove = self
            .workers
            .iter()
            .filter_map(|(key, workers)| {
                if let Some(elapsed) = workers.values().map(|w | w.last_received).max().map(|inst| now.duration_since(inst)) {
                    (elapsed > limit).then(|| key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for key in to_remove {
            if let Some(workers) = self.workers.remove(&key) {
                tracing::info!("parition_routed: disposing all workers assigned to partition: {key}");

                for (_, worker) in workers {
                    // dispose without further blocking
                    tokio::spawn(async move { worker.dispose().await });
                }
            }
        }
    }

    #[cfg(test)]
    pub fn set_stable_seed(&mut self) {
        self.hasher = ahash::RandomState::with_seeds(0x3038, 0x3039, 0x9394, 0x1234);
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
    use crate::test::{TestIncomingMessage, TestMessage, TestMessageBus};

    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    fn pr_config(duration: Duration) -> PartitionRoutedPoolConfig {
        PartitionRoutedPoolConfig {
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

    #[tokio::test]
    async fn partition_routed_pool_recreate_workers() {
        let (tx, mut rx) = unbounded_channel::<(TestMessage, WorkerId, usize)>();

        let extensions = Extensions::default().insert(tx);
        let router = Router::default().message_handler(handler);

        let context = WorkerContext::new(router, extensions, Default::default(), Json);
        let dur = Duration::from_millis(5);
        let mut workers = WorkerPool::<TestMessageBus, _>::partition_routed(pr_config(dur), context);

        let message = TestMessage::new(0);
        let incoming = TestIncomingMessage::create_raw_json(message.clone());
        workers.dispatch(incoming.clone()).await;

        tokio::time::sleep(dur * 2).await;
        workers.do_cleanup(Instant::now());

        match &workers.pool {
            Flavour::PartitionRouted(kr) => {
                assert_eq!(kr.workers.len(), 0, "expected worker to be recreated")
            }
            _ => unreachable!("partition routed pool is used"),
        };

        workers.dispatch(incoming.clone()).await;

        match &workers.pool {
            Flavour::PartitionRouted(kr) => {
                assert_eq!(kr.workers.len(), 1, "expected worker to be recreated")
            }
            _ => unreachable!("partition routed pool is used"),
        };

        assert_eq!(rx.recv().await.unwrap().2, 0);
        assert_eq!(
            rx.recv().await.unwrap().2,
            0,
            "previous worker was not removed (old TLS)"
        );
    }
}