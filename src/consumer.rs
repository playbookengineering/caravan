use std::{
    convert::Infallible,
    error::Error,
    fmt::Display,
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use self::{
    router::Router,
    worker::{WorkerContext, WorkerPool},
};

mod context;
mod extension;
mod extract;
mod handler;
mod message_bus;
mod router;
mod sentinel;
mod task_local;
mod worker;

use futures_lite::{Stream, StreamExt};

pub use context::ProcessContext;
pub use extension::Extension;
pub use extract::TryExtract;
pub use handler::{Handler, RoutableHandler};
pub use hook::Hook;
pub use message_bus::{IncomingMessage, MessageBus};
pub use sentinel::Sentinel;
#[cfg(feature = "unstable-features")]
pub use task_local::TaskLocal;
pub use worker::{FixedPoolConfig, KeyRoutedPoolConfig, WorkerPoolConfig};

pub(crate) use extension::Extensions;
pub(crate) use hook::Hooks;

pub mod hook;

use thiserror::Error;
use tokio::time::Interval;

#[derive(Debug, Error)]
pub enum MessageConsumerError {
    #[error("Failed sentinels: {0:?}")]
    SentinelError(Vec<Box<dyn Sentinel>>),

    #[error("Message bus error: {0}")]
    MessageBusError(Box<dyn Error + Send + Sync>),

    #[error("Message bus EOF")]
    MessageBusEOF,
}

#[derive(Default)]
pub struct MessageConsumer {
    router: Router,
    extensions: Extensions,
    hooks: Hooks,
}

impl MessageConsumer {
    pub fn message_handler<Fun, Args>(self, handler: Fun) -> Self
    where
        Fun: RoutableHandler<Args>
            + Handler<Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
        Args: Send + Sync + 'static,
    {
        Self {
            router: self.router.message_handler(handler),
            ..self
        }
    }

    pub fn fallback_handler<Fun, Args>(self, handler: Fun) -> Self
    where
        Fun: Handler<Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>> + 'static,
        Args: Send + Sync + 'static,
    {
        Self {
            router: self.router.fallback_handler(handler),
            ..self
        }
    }

    pub fn extension<T>(self, extension: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        Self {
            extensions: self.extensions.insert(extension),
            ..self
        }
    }

    pub fn hook<T>(self, hook: T) -> Self
    where
        T: Hook,
    {
        Self {
            hooks: self.hooks.push(hook),
            ..self
        }
    }

    pub async fn listen<B: MessageBus>(
        mut self,
        config: WorkerPoolConfig,
        bus: B,
    ) -> Result<Infallible, MessageConsumerError> {
        let sentinels = mem::take(&mut self.router.sentinels);

        let mut abortable = sentinels
            .into_iter()
            .filter(|x| x.abort(&self))
            .collect::<Vec<_>>();

        if !abortable.is_empty() {
            abortable.sort_by_key(|s| s.cause());
            abortable.dedup_by_key(|s| s.cause());

            return Err(MessageConsumerError::SentinelError(abortable));
        }

        let ctx = WorkerContext::new(self);

        //let cleanup_timer = config.timer();
        //let mut cleanup_tick_stream = TickStream(cleanup_timer);
        let mut worker_pool: WorkerPool<B::IncomingMessage> = WorkerPool::new(config, ctx.clone());
        let mut stream = bus
            .into_stream()
            .await
            .map_err(|err| MessageConsumerError::MessageBusError(err.into()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|err| {
                tracing::error!("Message bus error: {err}");
                MessageConsumerError::MessageBusError(err.into())
            })?;

            worker_pool.dispatch(msg).await;
        }

        Err(MessageConsumerError::MessageBusEOF)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
    Ack,
    Nack,
    Reject,
}

impl From<()> for Confirmation {
    fn from(_: ()) -> Self {
        Self::Ack
    }
}

impl<T: Into<Confirmation>, E: Into<Confirmation>> From<Result<T, E>> for Confirmation {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(ok) => ok.into(),
            Err(err) => err.into(),
        }
    }
}

impl Display for Confirmation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Confirmation::Ack => "ack",
            Confirmation::Nack => "nack",
            Confirmation::Reject => "reject",
        };

        f.write_str(s)
    }
}

struct TickStream(Option<Interval>);

impl Stream for TickStream {
    type Item = Instant;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(interval) = &mut self.0 {
            return match interval.poll_tick(cx) {
                Poll::Ready(i) => Poll::Ready(Some(i.into_std())),
                Poll::Pending => Poll::Pending,
            };
        }

        Poll::Ready(None)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        consumer::worker::FixedPoolConfig,
        message::RawMessage,
        test::{TestMessage, TestMessageBus},
    };

    use super::*;

    #[tokio::test]
    async fn detect_missing_extensions() {
        async fn fallback_handler_missing_states(
            _msg: RawMessage,
            _s1: Extension<()>,
            _s2: Extension<((), ())>,
        ) {
        }

        async fn message_handler_missing_states(
            _msg: TestMessage,
            _s1: Extension<()>,
            _s2: Extension<((), ())>,
        ) {
        }

        let consumer = MessageConsumer::default()
            .message_handler(message_handler_missing_states)
            .fallback_handler(fallback_handler_missing_states);

        let _error = consumer
            .listen(
                WorkerPoolConfig::Fixed(FixedPoolConfig {
                    count: 10,
                    queue_size: 128,
                }),
                TestMessageBus,
            )
            .await
            .unwrap_err();
    }
}
