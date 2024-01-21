mod builder;

pub use self::builder::AsyncConnectionBuilder;
use crate::Error;
use crate::SyncWrapper;
use std::sync::Arc;

enum Message {
    Access {
        func: Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>,
    },
    Close {
        tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

/// An async rusqlite connection.
#[derive(Debug, Clone)]
pub struct AsyncConnection {
    inner: Arc<InnerAsyncConnection>,
}

impl AsyncConnection {
    /// Get a builder for an [`AsyncConnection`].
    pub fn builder() -> AsyncConnectionBuilder {
        AsyncConnectionBuilder::new()
    }

    /// Close the database.
    ///
    /// This will queue a close request.
    /// When the database processes the close request,
    /// all current queued requests will be aborted.
    ///
    /// When this function returns,
    /// the database will be closed no matter the value of the return.
    /// The return value will return errors that occured while closing.
    pub async fn close(&self) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner
            .tx
            .send(Message::Close { tx })
            .map_err(|_| Error::Aborted)?;
        rx.await.map_err(|_| Error::Aborted)??;
        Ok(())
    }

    /// Access the database.
    ///
    /// Note that dropping the returned future will not cancel the database access.
    pub async fn access<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        // TODO: We should make this a function and have it return a named Future.
        // This will allow users to avoid spawning a seperate task for each database call.

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner
            .tx
            .send(Message::Access {
                func: Box::new(move |connection| {
                    // TODO: Consider aborting if rx hung up.

                    let func = std::panic::AssertUnwindSafe(|| func(connection));
                    let result = std::panic::catch_unwind(func);
                    let result = result
                        .map_err(|panic_data| Error::AccessPanic(SyncWrapper::new(panic_data)));
                    let _ = tx.send(result).is_ok();
                }),
            })
            .map_err(|_| Error::Aborted)?;
        let result = rx.await.map_err(|_| Error::Aborted)??;
        Ok(result)
    }
}

#[derive(Debug)]
struct InnerAsyncConnection {
    tx: std::sync::mpsc::Sender<Message>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::_assert_send;
    use crate::test::_assert_sync;

    const _MESSAGE_IS_SEND: () = _assert_send::<Message>();

    const _INNER_ASYNC_CONNECTION_IS_SEND: () = _assert_send::<InnerAsyncConnection>();
    const _INNER_ASYNC_CONNECTION_IS_SYNC: () = _assert_sync::<InnerAsyncConnection>();

    const _ASYNC_CONNECTION_IS_SEND: () = _assert_send::<AsyncConnection>();
    const _ASYNC_CONNECTION_IS_SYNC: () = _assert_sync::<AsyncConnection>();
}
