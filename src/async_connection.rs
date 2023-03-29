use crate::Error;
use crate::SyncWrapper;
use std::path::Path;

const DEFAULT_MESSAGE_CHANNEL_CAPACITY: usize = 32;

enum Message {
    Access {
        func: Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>,
    },
    Close {
        tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

/// An async rusqlite connection.
#[derive(Debug)]
pub struct AsyncConnection {
    tx: tokio::sync::mpsc::Sender<Message>,
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
            .await
            .map_err(|_| Error::Aborted)?;
        rx.await.map_err(|_| Error::Aborted)??;
        Ok(())
    }

    /// Access the database.
    ///
    /// Note that dropping the returned future will no cancel the database access.
    pub async fn access<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Message::Access {
                func: Box::new(move |connection| {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| func(connection)))
                            .map_err(|panic_data| Error::AccessPanic(SyncWrapper::new(panic_data)));
                    let _ = tx.send(result).is_ok();
                }),
            })
            .await
            .map_err(|_| Error::Aborted)?;

        let result = rx.await.map_err(|_| Error::Aborted)??;

        Ok(result)
    }
}

/// A builder for an [`AsyncConnection`].
#[derive(Debug)]
pub struct AsyncConnectionBuilder {
    /// The message channel capacity for the background database thread.
    pub message_channel_capacity: usize,
}

impl AsyncConnectionBuilder {
    /// Create an [`AsyncConnectionBuilder`] with default settings.
    pub fn new() -> Self {
        Self {
            message_channel_capacity: DEFAULT_MESSAGE_CHANNEL_CAPACITY,
        }
    }

    /// Set the message channel capacity.
    pub fn message_channel_capacity(&mut self, capacity: usize) -> &mut Self {
        self.message_channel_capacity = capacity;
        self
    }

    /// Open the connection.
    pub async fn open<P>(&self, path: P) -> Result<AsyncConnection, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(self.message_channel_capacity);
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
            while let Some(message) = rx.blocking_recv() {
                match message {
                    Message::Close { tx } => {
                        rx.close();
                        close_tx = Some(tx);
                        break;
                    }
                    Message::Access { func } => {
                        func(&mut connection);
                    }
                }
            }

            // Drain messages, dropping them without sending a response.
            // This is considered aborting the request.
            while let Some(_message) = rx.blocking_recv() {}

            let result = connection.close();
            if let Some(tx) = close_tx {
                let _ = tx
                    .send(result.map_err(|(_connection, error)| Error::from(error)))
                    .is_ok();
            }
        });

        connection_open_rx.await.map_err(|_| Error::Aborted)??;

        Ok(AsyncConnection { tx })
    }
}

impl Default for AsyncConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}
