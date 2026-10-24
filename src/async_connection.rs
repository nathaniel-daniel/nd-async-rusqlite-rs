mod builder;

pub use self::builder::AsyncConnectionBuilder;
use crate::Error;
use crate::SyncWrapper;
use std::path::Path;
use std::sync::Arc;

enum Message {
    Access {
        func: Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>,
    },
    Close {
        tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

#[derive(Debug)]
struct InnerAsyncConnection {
    tx: std::sync::mpsc::Sender<Message>,
}

/// An asynchronous interface to a SQLite connection on a single background thread.
///
/// `AsyncConnection` executes database operations on a single dedicated background
/// thread, allowing async code to use [`rusqlite`] without blocking.
///
/// ```no_run
/// use nd_async_rusqlite::AsyncConnection;
///
/// # Examples
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let connection = AsyncConnection::open("example.db").await?;
///
///     connection
///         .access(|conn| {
///             conn.execute(
///                 "CREATE TABLE IF NOT EXISTS users (
///                     id INTEGER PRIMARY KEY,
///                     name TEXT NOT NULL
///                 );",
///                 [],
///             )
///         })
///         .await??;
///
///    connection.close()?;
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct AsyncConnection {
    inner: Arc<InnerAsyncConnection>,
}

impl AsyncConnection {
    /// Open a new [`AsyncConnection`].
    pub async fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        AsyncConnectionBuilder::new().open(path).await
    }

    /// Get a builder for an [`AsyncConnection`].
    pub fn builder() -> AsyncConnectionBuilder {
        AsyncConnectionBuilder::new()
    }

    /// Close the database.
    ///
    /// This will queue a close request.
    /// When the database processes the close request,
    /// all current in-flight requests will be aborted.
    ///
    /// When this function returns,
    /// the database will be closed no matter the value of the return.
    /// The return value will return errors that occured while closing.
    ///
    /// # Examples
    /// ```no_run
    /// connection.close().await?;
    ///
    /// // This will return an error.
    /// connection
    ///     .access(|conn| {
    ///         conn.execute(
    ///             "CREATE TABLE IF NOT EXISTS users (
    ///                 id INTEGER PRIMARY KEY,
    ///                 name TEXT NOT NULL
    ///             );",
    ///             [],
    ///         )
    ///     })
    ///     .await??;
    /// ```
    pub async fn close(&self) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner
            .tx
            .send(Message::Close { tx })
            .map_err(|_| Error::Aborted)?;
        rx.await.map_err(|_| Error::Aborted)??;
        Ok(())
    }

    /// Executes a closure with mutable access to the database connection.
    ///
    /// The closure is executed on the connection's background thread. The
    /// returned future resolves to the closure's return value once execution
    /// completes.
    ///
    /// # Cancellation
    /// Dropping the returned future will NOT cancel the database access.
    ///
    /// # Panics
    ///
    /// If the closure panics, the panic is caught and returned as
    /// [`Error::AccessPanic`].
    ///
    /// # Examples
    /// ```no_run
    /// use nd_async_rusqlite::AsyncConnection;
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let connection = AsyncConnection::open("example.db").await?;
    ///
    ///     connection
    ///         .access(|conn| {
    ///             conn.execute(
    ///                 "CREATE TABLE IF NOT EXISTS users (
    ///                     id INTEGER PRIMARY KEY,
    ///                     name TEXT NOT NULL
    ///                 );",
    ///                 [],
    ///             )
    ///         })
    ///         .await??;
    ///
    ///     Ok(())
    /// }
    /// ```
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
