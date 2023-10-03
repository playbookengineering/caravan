use std::{
    convert::Infallible,
    error::Error,
    fmt::Display,
    future::Future,
    marker::PhantomData,
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
    SentinelError(Vec<String>),

    #[error("Message bus error: {0}")]
    MessageBusError(Box<dyn Error + Send + Sync>),

    #[error("Message bus EOF")]
    MessageBusEOF,
}

/// [`MessageConsumer`] handles messages from [`MessageBus`] similarly to how http servers handle requests.
/// It allows for registering handlers for `Message` as in http servers where we register handlers for given path.
/// Message kind is simple string which identifies given message type.
/// Based on that kind, message is routed to proper handler.
///
/// [`MessageConsumer`] also allows for registering extensions which are shared across all handlers.
/// Such extensions might be for example a database connection, channels etc.
///
/// # Example
///
/// ```ignore
/// async fn hello_message(msg: MyMessage) {
///     tracing::info!("Hello, {msg:?}");
/// }
///
/// let consumer = MessageConsumer::new()
///     .message_handler(hello_example)
///     .listen(bus, WorkerPoolConfig::fixed(10)).await?;
/// ```
pub struct MessageConsumer<B: MessageBus> {
    router: Router<B>,
    extensions: Extensions,
    hooks: Hooks<B>,
    bus: PhantomData<B>,
}

impl<B: MessageBus> Default for MessageConsumer<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: MessageBus> MessageConsumer<B> {
    /// Returns new [`MessageConsumer`]
    pub fn new() -> Self {
        Self {
            router: Router::default(),
            extensions: Extensions::default(),
            hooks: Hooks::default(),
            bus: PhantomData,
        }
    }

    /// Registers message handler
    ///
    /// # Handler requiremenets:
    ///
    /// - must take message as the first parameter (type which implements [`Message`] trait)
    /// - following parameters must implement [`TryExtract`] trait
    /// - must return a `Send` future
    /// - future output must be convertible to [`Confirmation`]
    ///
    /// We can form handlers as an `async` function or a closure:
    ///
    /// ```ignore
    /// |_msg: MyMessage| async move { Confirmation::Ack }
    /// ```
    ///
    /// ```ignore
    /// async fn my_message_handler(_message: MyMessage) {
    ///     Confirmation::Ack
    /// }
    /// ```
    ///
    /// # Example handlers:
    ///
    /// * infallible
    ///
    /// ```ignore
    /// async fn my_message_handler(message: MyMessage) {
    ///     let _ = message; // do something with message
    ///
    ///     // `()` is converted to `Confirmation::Ack`
    /// }
    /// ```
    ///
    /// * fallible
    ///
    /// ```ignore
    /// pub enum MyError {
    ///   Database(DbError),
    /// }
    ///
    /// (impl Error boilerplate...)
    ///
    /// impl From<MyError> for Confirmation {
    ///     fn from(_: MyError) -> Confirmation {
    ///         // dependency failed, don't dequeue message
    ///         Confirmation::Nack
    ///     }
    /// }
    ///
    /// async fn my_message_handler(
    ///   message: MyMessage,
    ///   Extension(db): Extension<SomeDatabase>)
    /// -> Result<(), MyError> {
    ///     let db_conn = state.db.acquire().await.map_err(MyError::Database)?;
    ///
    ///     let _ = message; // do something with message
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn message_handler<Fun, Args>(self, handler: Fun) -> Self
    where
        Fun: RoutableHandler<B, Args>
            + Handler<B, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>>
            + 'static,
        Args: Send + Sync + 'static,
    {
        Self {
            router: self.router.message_handler(handler),
            ..self
        }
    }

    /// Replaces fallback handler
    ///
    /// Fallback handler is called when message can't be routed. This can occurr if:
    ///
    /// - `x-convoy-kind` header is missing
    /// - handler of given kind is not registered
    ///
    /// # Fallback handler requirements:
    ///
    /// - must take [`RawMessage`] as the first parameter
    /// - following parameters must implement [`TryExtract`] trait
    /// - must return a `Send` future
    /// - future output must be convertible to [`Confirmation`]
    pub fn fallback_handler<Fun, Args>(self, handler: Fun) -> Self
    where
        Fun:
            Handler<B, Args, Future = Pin<Box<dyn Future<Output = Confirmation> + Send>>> + 'static,
        Args: Send + Sync + 'static,
    {
        Self {
            router: self.router.fallback_handler(handler),
            ..self
        }
    }

    /// Installs extensions
    ///
    /// These are used to share state across handlers and can be extracted
    /// by using [`Extension`] extractor.
    ///
    /// # Extension requirements:
    ///
    /// Extension must be `Clone + Send + Sync + 'static`
    ///
    /// # Example
    ///
    /// ```ignore
    /// // define extension
    /// #[derive(Clone)]
    /// struct State {
    ///     database: MyDatabase
    /// }
    ///
    /// // define handler with extension access
    /// async fn my_message_handler(_msg: MyMessage, Extension(_state): Extension<State>) {
    ///
    /// }
    ///
    /// // install extension and handler
    /// let consumer = MessageConsumer::new()
    ///     .extension(MyExtension)
    ///     .message_handler(my_message_handler)
    ///     .listen(bus, WorkerPoolConfig::fixed(10)).await
    /// ```
    pub fn extension<T>(self, extension: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        Self {
            extensions: self.extensions.insert(extension),
            ..self
        }
    }

    /// Installs hook
    ///
    /// These are called before and after message processing
    ///
    /// # Hook requirements
    ///
    /// Must implement [`Hook`] trait
    ///
    /// # Example
    ///
    /// ```ignore
    /// use chrono::Utc;
    ///
    /// // define hook
    /// struct Timings;
    ///
    /// struct RecordedTime(i64);
    ///
    /// impl<B: MessageBus> Hook for Timings {
    ///     fn before_processing(&self, ctx: &ProcessContext<'_, B>) {
    ///         let now = Utc::now().timestamp_millis();
    ///
    ///         ctx.cache_mut.set(RecordedTime(now));
    ///     }
    ///
    ///     fn after_processing(&self, ctx: &ProcessContext<'_, B>, _: Confirmation) {
    ///         let now = Utc::now().timestamp_millis();
    ///         let start: Option<RecordedTime> = ctx.cache.get();
    ///
    ///         if let Some(start) = start {
    ///             tracing::info!("Took: {} ms", now - start.0);
    ///         }
    ///     }
    /// }
    ///
    /// // install hook
    /// let consumer = MessageConsumer::new()
    ///     .hook(Timings)
    /// ```
    pub fn hook<T>(self, hook: T) -> Self
    where
        T: Hook<B>,
    {
        Self {
            hooks: self.hooks.push(hook),
            ..self
        }
    }

    /// Finalize consumer configuration and start listening
    ///
    /// As a final step you need to choose between two concurrency strategies:
    ///
    /// - fixed worker pool - create pool of N workers:
    ///
    /// ```ignore
    /// MessageConsumer::new()
    ///     .listen(WorkerPoolConfig::fixed(10))
    ///     .await
    /// ```
    ///
    /// - key-routed - workers are created per _message key_. Cleanup of inactive workers
    ///   will occurr after each tick of `inactivity_duration`:
    ///
    /// ```ignore
    /// MessageConsumer::new()
    ///     .listen(WorkerPoolConfig::key_routed(Duration::from_secs(5 * 60)))
    ///     .await
    /// ```
    ///
    /// *NOTE*: key-routed strategy allows for creating stateful handlers
    /// with use of [`TaskLocal`] extractor. However, this library function is not
    /// stable yet and the API is a subject to change.
    /// For this reason fixed strategy is recommended at the moment
    pub async fn listen(
        mut self,
        bus: B,
        config: WorkerPoolConfig,
    ) -> Result<Infallible, MessageConsumerError> {
        let sentinels = mem::take(&mut self.router.sentinels);

        let mut abortable = sentinels
            .into_iter()
            .filter_map(|x| x.abort(&self).then(|| x.cause()))
            .collect::<Vec<_>>();

        if !abortable.is_empty() {
            abortable.sort();
            abortable.dedup();

            return Err(MessageConsumerError::SentinelError(abortable));
        }

        let Self {
            router,
            extensions,
            hooks,
            bus: _,
        } = self;

        let ctx = WorkerContext::new(router, extensions, hooks);

        let cleanup_timer = config.timer();
        let mut cleanup_tick_stream = TickStream(cleanup_timer);
        let mut worker_pool: WorkerPool<B> = WorkerPool::new(config, ctx);
        let mut stream = bus
            .into_stream()
            .await
            .map_err(|err| MessageConsumerError::MessageBusError(err.into()))?;

        loop {
            tokio::select! {
                biased;
                msg = stream.next() => {
                    let msg = msg.ok_or(MessageConsumerError::MessageBusEOF)?.map_err(|err| MessageConsumerError::MessageBusError(err.into()))?;
                    worker_pool.dispatch(msg).await;
                }
                Some(tick) = cleanup_tick_stream.next() => {
                    worker_pool.do_cleanup(tick);
                }
            }
        }
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

        let consumer = MessageConsumer::<TestMessageBus>::new()
            .message_handler(message_handler_missing_states)
            .fallback_handler(fallback_handler_missing_states);

        let _error = consumer
            .listen(
                TestMessageBus,
                WorkerPoolConfig::Fixed(FixedPoolConfig {
                    count: 10,
                    queue_size: 128,
                }),
            )
            .await
            .unwrap_err();
    }
}
