use crate::Error;
use crate::SyncWrapper;
use std::path::Path;
use std::path::PathBuf;

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
    tx: std::sync::mpsc::Sender<Message>,
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
        self.tx
            .send(Message::Close { tx })
            .map_err(|_| Error::Aborted)?;
        rx.await.map_err(|_| Error::Aborted)??;
        Ok(())
    }

    /// Internal function for accessing the database.
    fn access_internal<F, T>(
        &self,
        func: F,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<T, Error>>, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
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

        Ok(rx)
    }

    /// Access the database.
    ///
    /// Note that dropping the returned future will no cancel the database access.
    pub async fn access<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let rx = self.access_internal(func)?;
        let result = rx.await.map_err(|_| Error::Aborted)??;
        Ok(result)
    }

    /// Access the database from a blocking context.
    pub fn blocking_access<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let rx = self.access_internal(func)?;
        let result = rx.blocking_recv().map_err(|_| Error::Aborted)??;
        Ok(result)
    }
}

/// A builder for an [`AsyncConnection`].
#[derive(Debug)]
pub struct AsyncConnectionBuilder {}

impl AsyncConnectionBuilder {
    /// Create an [`AsyncConnectionBuilder`] with default settings.
    pub fn new() -> Self {
        Self {}
    }

    fn open_internal(
        &self,
        path: PathBuf,
    ) -> (
        AsyncConnection,
        tokio::sync::oneshot::Receiver<Result<(), rusqlite::Error>>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel::<Message>();
        let (connection_open_tx, connection_open_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut connection = match rusqlite::Connection::open(path) {
                Ok(connection) => {
                    // Check if the user cancelled the opening of the database connection and return early if needed.
                    if connection_open_tx.send(Ok(())).is_err() {
                        return;
                    }

                    connection
                }
                Err(error) => {
                    // Don't care if we succed since we should exit in either case.
                    let _ = connection_open_tx.send(Err(error)).is_ok();
                    return;
                }
            };

            let mut close_tx = None;
            while let Ok(message) = rx.recv() {
                match message {
                    Message::Close { tx } => {
                        close_tx = Some(tx);
                        break;
                    }
                    Message::Access { func } => {
                        func(&mut connection);
                    }
                }
            }

            // Drop rx.
            // This will abort all queued messages, dropping them without sending a response.
            // This is considered aborting the request.
            drop(rx);

            let result = connection.close();
            if let Some(tx) = close_tx {
                let _ = tx
                    .send(result.map_err(|(_connection, error)| Error::from(error)))
                    .is_ok();
            }
        });

        (AsyncConnection { tx }, connection_open_rx)
    }

    /// Open the connection.
    pub async fn open<P>(&self, path: P) -> Result<AsyncConnection, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        let (async_connection, connection_open_rx) = self.open_internal(path);
        connection_open_rx.await.map_err(|_| Error::Aborted)??;

        Ok(async_connection)
    }

    /// Open the connection from a blocking context.
    pub fn blocking_open<P>(&self, path: P) -> Result<AsyncConnection, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        let (async_connection, connection_open_rx) = self.open_internal(path);
        connection_open_rx
            .blocking_recv()
            .map_err(|_| Error::Aborted)??;

        Ok(async_connection)
    }
}

impl Default for AsyncConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}
