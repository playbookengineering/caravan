use std::{
    cell::Cell,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    mem::{self, ManuallyDrop},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures_lite::{Stream, StreamExt};
use rdkafka::{
    consumer::{
        CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, MessageStream,
        StreamConsumer,
    },
    error::KafkaError,
    message::{BorrowedMessage, Headers, Message as _Message},
    TopicPartitionList,
};

use crate::{
    consumer::{IncomingMessage, MessageBus},
    message::RawHeaders,
};
pub struct RdKafkaMessageStream<C>
where
    C: ConsumerContext + 'static,
{
    consumer: ManuallyDrop<Arc<StreamConsumer<C>>>,
    stream: ManuallyDrop<MessageStream<'static>>,
    commit_queue: Arc<Option<Mutex<CommitQueue>>>,
}

impl<C: ConsumerContext> RdKafkaMessageStream<C> {
    /// Constructs new `RdKafkaMessageStream`
    ///
    /// SAFETY: `stream` must originate from `consumer`
    unsafe fn new<'a>(
        consumer: &'a Arc<StreamConsumer<C>>,
        stream: MessageStream<'a>,
        commit_queue: Arc<Option<Mutex<CommitQueue>>>,
    ) -> Self {
        let consumer = Arc::clone(consumer);

        let stream = mem::transmute::<_, MessageStream<'static>>(stream);

        Self {
            consumer: ManuallyDrop::new(consumer),
            stream: ManuallyDrop::new(stream),
            commit_queue,
        }
    }
}

impl<C: ConsumerContext> Drop for RdKafkaMessageStream<C> {
    fn drop(&mut self) {
        // SAFETY: By preserving order (stream first, consumer second)
        // we guarantee that `message` still points to valid memory
        // allocated by rdkafka
        unsafe {
            ManuallyDrop::drop(&mut self.stream);
            ManuallyDrop::drop(&mut self.consumer);
        }
    }
}

impl<C: ConsumerContext> Stream for RdKafkaMessageStream<C> {
    type Item = Result<RdKafkaOwnedMessage<C>, KafkaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next(cx).map_ok(|message| unsafe {
            println!("poll_next: partition {}, offset: {}", message.partition(), message.offset());

            RdKafkaOwnedMessage::new(&self.consumer, message, Arc::clone(&self.commit_queue))
        })
    }
}

pub struct RdKafkaOwnedMessage<C>
where
    C: ConsumerContext + 'static,
{
    consumer: ManuallyDrop<Arc<StreamConsumer<C>>>,
    message: ManuallyDrop<BorrowedMessage<'static>>,
    headers: RawHeaders,
    commit_queue: Arc<Option<Mutex<CommitQueue>>>,
}

impl<C: ConsumerContext> RdKafkaOwnedMessage<C> {
    /// Constructs new `RdkafkaOwnedMessage`
    ///
    /// SAFETY: `message` must originate from `consumer`
    unsafe fn new<'a>(
        consumer: &'a Arc<StreamConsumer<C>>,
        message: BorrowedMessage<'a>,
        commit_queue: Arc<Option<Mutex<CommitQueue>>>,
    ) -> Self {
        let tp = TopicPartition {
            topic: message.topic().to_owned(),
            partition: message.partition(),
        };

        if let Some(cq) = commit_queue.as_ref() {
            cq.lock()
                .expect("Mutex poisoned")
                .push(tp, message.offset());

            tracing::debug!("commit [partition: {:?}], reserved: {}, pointer: {:?}", message.partition(), message.offset(), Arc::as_ptr(consumer));
        }

        let consumer = Arc::clone(consumer);

        // SAFETY: since we have `consumer` for 'static we can extend
        // message lifetime
        let message = mem::transmute::<_, BorrowedMessage<'static>>(message);

        // process headers
        let headers = message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .filter_map(|header| {
                        let value = header.value?;
                        let value = std::str::from_utf8(value).ok()?;

                        let key = header.key.to_string();
                        let value = value.to_string();

                        Some((key, value))
                    })
                    .collect::<RawHeaders>()
            })
            .unwrap_or_default();

        Self {
            consumer: ManuallyDrop::new(consumer),
            message: ManuallyDrop::new(message),
            headers,
            commit_queue,
        }
    }

    pub fn message(&self) -> &BorrowedMessage<'_> {
        &self.message
    }

    pub fn commit(&self) -> Result<(), rdkafka::error::KafkaError> {
        if let Some(cq) = self.commit_queue.as_ref() {
            let topic = self.message.topic();
            let partition = self.message.partition();
            let offset = self.message.offset();

            let tp = TopicPartition {
                topic: topic.to_owned(),
                partition,
            };

            // hold the lock until we sync with consumer
            {
                let mut cq = cq.lock().expect("Poisoned mutex");

                if let Some(offset) = cq.commit(tp, offset) {
                    tracing::debug!("commit [partition: {:?}], executing {}", partition, offset);

                    let mut tpl = TopicPartitionList::with_capacity(1);
                    tpl.add_partition_offset(
                        topic,
                        partition,
                        rdkafka::Offset::Offset(offset + 1),
                    )?;
                    self.consumer.commit(&tpl, CommitMode::Async)?;
                } else {
                    tracing::debug!("commit [partition: {:?}], skipping {}", partition, offset);
                }
            }
        }

        Ok(())
    }
}

impl<C: ConsumerContext> Drop for RdKafkaOwnedMessage<C> {
    fn drop(&mut self) {
        // SAFETY: By preserving order (message first, consumer second)
        // we guarantee that `message` still points to valid memory
        // allocated by rdkafka
        unsafe {
            ManuallyDrop::drop(&mut self.message);
            ManuallyDrop::drop(&mut self.consumer);
        }
    }
}

pub struct KafkaConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext + 'static,
{
    consumer: Arc<StreamConsumer<C>>,
    commit: bool,
}

impl<C> KafkaConsumer<C>
where
    C: ConsumerContext + 'static,
{
    pub fn new(consumer: StreamConsumer<C>) -> Self {
        Self {
            consumer: Arc::new(consumer),
            commit: true,
        }
    }

    pub fn with_commit(mut self, commit: bool) -> Self {
        self.commit = commit;
        self
    }
}

#[async_trait]
impl<C: ConsumerContext + 'static> MessageBus for KafkaConsumer<C> {
    type IncomingMessage = RdKafkaOwnedMessage<C>;
    type Error = rdkafka::error::KafkaError;
    type Stream = RdKafkaMessageStream<C>;

    async fn into_stream(self) -> Result<Self::Stream, Self::Error> {
        let stream = self.consumer.stream();
        let commit_queue = Arc::new(
            self.commit
                .then(|| Some(Default::default()))
                .unwrap_or_default(),
        );
        let stream = unsafe { RdKafkaMessageStream::new(&self.consumer, stream, commit_queue) };

        Ok(stream)
    }
}

#[async_trait]
impl<C: ConsumerContext + 'static> IncomingMessage for RdKafkaOwnedMessage<C> {
    type Error = KafkaError;

    fn headers(&self) -> &RawHeaders {
        &self.headers
    }

    fn payload(&self) -> &[u8] {
        self.message().payload().unwrap_or_default()
    }

    fn key(&self) -> Option<&[u8]> {
        self.message.key()
    }

    fn partition(&self) -> Option<i32> {
       Some(self.message.partition())
    }

    async fn ack(&self) -> Result<(), Self::Error> {
        self.commit()
    }

    async fn nack(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn reject(&self) -> Result<(), Self::Error> {
        self.commit()
    }

    fn make_span(&self) -> tracing::Span {
        let msg = self.message();

        // https://opentelemetry.io/docs/specs/otel/trace/semantic_conventions/messaging/#apache-kafka
        tracing::info_span!(
            "consumer",
            otel.name = %format!("{} receive", msg.topic()).as_str(),
            otel.kind = "CONSUMER",
            otel.status_code = tracing::field::Empty,
            messaging.system = "kafka",
            messaging.operation = "receive",
            messaging.message.payload_size_bytes = msg.payload_len(),
            messaging.kafka.source.partition = msg.partition(),
            messaging.kafka.message.key = msg.key().and_then(|k| std::str::from_utf8(k).ok()).unwrap_or_default(),
            messaging.kafka.message.offset = msg.offset(),
            convoy.kind = tracing::field::Empty,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopicPartition {
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Offset {
    offset: i64,
    committed: Cell<bool>,
}

impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Offset {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

#[derive(Default, Debug)]
struct CommitQueue {
    topic_partition_queue: HashMap<TopicPartition, BinaryHeap<Reverse<Offset>>>,
}

impl CommitQueue {
    fn push(&mut self, tp: TopicPartition, offset: i64) {
        let heap = self.topic_partition_queue.entry(tp).or_default();

        heap.push(Reverse(Offset {
            offset,
            committed: false.into(),
        }));
    }

    fn commit(&mut self, tp: TopicPartition, offset: i64) -> Option<i64> {
        let heap = self.topic_partition_queue.entry(tp).or_default();

        if let Some(offset) = heap.iter().find(|x| x.0.offset == offset) {
            offset.0.committed.set(true);
        }

        let mut max_commited = None;

        loop {
            let mut pop = false;
            match heap.peek_mut() {
                Some(mut o) => {
                    let next = &mut o.0;

                    if next.committed.get() {
                        max_commited = Some(next.offset);
                        pop = true;
                    }
                }
                None => break,
            };

            if pop {
                heap.pop();
            } else {
                break;
            }
        }

        max_commited
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn commit_queue_inorder() {
        let mut cq = CommitQueue::default();

        let tp = TopicPartition {
            topic: "test".to_owned(),
            partition: 0,
        };

        for offset in 0..10 {
            cq.push(tp.clone(), offset);
            assert_eq!(
                cq.commit(tp.clone(), offset),
                Some(offset),
                "Failed at {offset} iteration"
            );
        }
    }

    #[test]
    fn commit_queue_out_of_order() {
        let mut cq = CommitQueue::default();

        let tp = TopicPartition {
            topic: "test".to_owned(),
            partition: 0,
        };

        // kafka messages are read in order
        cq.push(tp.clone(), 0);
        cq.push(tp.clone(), 1);
        cq.push(tp.clone(), 2);
        cq.push(tp.clone(), 3);

        // messages can be processed out of order (in terms of partition)

        // 1. heap after commit: {0:false, 1:false, 2:true, 3:false}, nothing to pop
        assert_eq!(cq.commit(tp.clone(), 2), None);

        // 2. heap after commit: {0:true, 1:false, 2:true, 3:false}, pop first
        assert_eq!(cq.commit(tp.clone(), 0), Some(0));

        // 3. heap after commit: {1: true, 2: true, 3: false}, pop 2 elements, return last
        assert_eq!(cq.commit(tp.clone(), 1), Some(2));

        // 4. heap after commit: {3: true}, return last element leaving heap empty
        assert_eq!(cq.commit(tp.clone(), 3), Some(3));
        assert!(cq.topic_partition_queue.get(&tp).unwrap().is_empty());
    }


    #[test]
    fn commit_queue_out_of_order_2() {
        let mut cq = CommitQueue::default();

        let tp = TopicPartition {
            topic: "test".to_owned(),
            partition: 0,
        };

        // kafka messages are read in order
        for i in 0..10 {
            cq.push(tp.clone(), i);
        }

        for i in 1..10 {
            assert_eq!(cq.commit(tp.clone(), i), None);
        }

        assert_eq!(cq.commit(tp.clone(), 0), Some(9));
        assert!(cq.topic_partition_queue.get(&tp).unwrap().is_empty());
    }

    #[test]
    fn commit_queue_out_of_order_3() {
        let mut cq = CommitQueue::default();

        let tp = TopicPartition {
            topic: "test".to_owned(),
            partition: 0,
        };

        // kafka messages are read in order
        for i in 10023..10055 {
            cq.push(tp.clone(), i);
        }


        assert_eq!(cq.commit(tp.clone(), 10112), None);
        assert_eq!(cq.commit(tp.clone(), 10121), None);
        assert_eq!(cq.commit(tp.clone(), 10110), None);
        assert_eq!(cq.commit(tp.clone(), 10105), None);
        assert_eq!(cq.commit(tp.clone(), 10105), None);
    }
}
