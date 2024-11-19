use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use convoy::{
    consumer::{IncomingMessage, MessageBus},
    message::{RawHeaders, RawMessage},
    producer::Producer,
};
use futures_lite::Stream;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const PRODUCE_SPAN_NAME: &str = "msg_produce";
pub const RECEIVE_SPAN_NAME: &str = "msg_receive";

pub fn make_queue() -> (InMemoryProducer, InMemoryMessageBus) {
    let (tx, rx) = channel(16);

    let producer = InMemoryProducer(tx);
    let message_bus = InMemoryMessageBus { rx };

    (producer, message_bus)
}

pub struct InMemoryMessageBus {
    rx: Receiver<InMemoryMessage>,
}

pub struct InMemoryMessageStream(Receiver<InMemoryMessage>);

impl Stream for InMemoryMessageStream {
    type Item = Result<InMemoryMessage, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx).map(|m| m.map(Ok))
    }
}

#[allow(unused)]
impl InMemoryMessageBus {
    pub fn into_receiver(self) -> Receiver<InMemoryMessage> {
        self.rx
    }
}

#[async_trait::async_trait]
impl MessageBus for InMemoryMessageBus {
    type IncomingMessage = InMemoryMessage;
    type Error = Infallible;
    type Stream = InMemoryMessageStream;

    async fn into_stream(self) -> Result<Self::Stream, Self::Error> {
        Ok(InMemoryMessageStream(self.rx))
    }
}

pub struct InMemoryMessage(pub Arc<RawMessage>);

#[async_trait]
impl IncomingMessage for InMemoryMessage {
    type Error = Infallible;

    fn headers(&self) -> &RawHeaders {
        &self.0.headers
    }

    fn payload(&self) -> &[u8] {
        &self.0.payload
    }

    fn partition(&self) -> Option<i32> {
        None
    }

    fn key(&self) -> Option<&[u8]> {
        self.0.key.as_ref().map(|k| &k[..])
    }

    fn make_span(&self) -> tracing::Span {
        tracing::info_span!(RECEIVE_SPAN_NAME, convoy.kind = tracing::field::Empty)
    }

    async fn ack(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn nack(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn reject(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct InMemoryProducer(Sender<InMemoryMessage>);

impl InMemoryProducer {
    pub fn into_sender(self) -> Sender<InMemoryMessage> {
        self.0
    }
}

#[async_trait]
impl Producer for InMemoryProducer {
    type Options = ();

    type Error = Infallible;

    async fn send(
        &self,
        key: String,
        headers: RawHeaders,
        payload: Vec<u8>,
        _: Self::Options,
    ) -> Result<(), Self::Error> {
        let message = InMemoryMessage(Arc::new(RawMessage {
            key: Some(key.into_bytes()),
            payload,
            headers,
        }));

        self.0.send(message).await.unwrap();

        Ok(())
    }

    fn make_span(
        &self,
        _key: &str,
        _headers: &RawHeaders,
        _payload: &[u8],
        _options: &Self::Options,
    ) -> tracing::Span {
        tracing::info_span!(PRODUCE_SPAN_NAME, convoy.kind = tracing::field::Empty)
    }
}
