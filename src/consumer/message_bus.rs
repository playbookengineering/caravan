use std::error::Error;

use async_trait::async_trait;
use futures_lite::Stream;

use crate::{
    codec::Codec,
    message::{Message, RawHeaders, TryFromRawHeaders},
};

use super::context::MessageConvertError;

#[async_trait]
pub trait MessageBus: Send + Sync + 'static {
    type IncomingMessage: IncomingMessage<Error = Self::Error>;
    type Error: Error + Send + Sync + 'static;
    type Stream: Stream<Item = Result<Self::IncomingMessage, Self::Error>> + Unpin;

    /// Returns stream of messages
    async fn into_stream(self) -> Result<Self::Stream, Self::Error>;
}

#[async_trait]
pub trait IncomingMessage: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    fn headers(&self) -> &RawHeaders;

    fn payload(&self) -> &[u8];

    fn key(&self) -> Option<&[u8]>;

    fn partition(&self) -> Option<i32>;

    fn make_span(&self) -> tracing::Span;

    /// Acknowledge message and dequeue
    async fn ack(&self) -> Result<(), Self::Error>;

    /// Negative acknowledgement, don't dequeue
    async fn nack(&self) -> Result<(), Self::Error>;

    /// Reject message and dequeue
    async fn reject(&self) -> Result<(), Self::Error>;
}

pub(crate) async fn extract_message<C, IM, M>(
    message: &IM,
    codec: C,
) -> Result<M, MessageConvertError>
where
    C: Codec,
    IM: IncomingMessage,
    M: Message,
{
    let headers: M::Headers = TryFromRawHeaders::try_from_raw_headers(message.headers().clone())
        .map_err(|err| MessageConvertError::Headers(Box::new(err)))?;

    let body: M::Body = codec
        .decode(message.payload())
        .await
        .map_err(|err| MessageConvertError::Codec(Box::new(err)))?;

    Ok(Message::from_body_and_headers(body, headers))
}
