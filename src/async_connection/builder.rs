use super::AsyncConnection;
use super::InnerAsyncConnection;
use super::Message;
use crate::Error;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

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
        std::thread::spawn(move || async_connection_thread_impl(rx, path, connection_open_tx));

        (
            AsyncConnection {
                inner: Arc::new(InnerAsyncConnection {
                    tx,
                    semaphore: None,
                }),
            },
            connection_open_rx,
        )
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

/// The impl for the async connection background thread.
fn async_connection_thread_impl(
    rx: std::sync::mpsc::Receiver<Message>,
    path: PathBuf,
    connection_open_tx: tokio::sync::oneshot::Sender<rusqlite::Result<()>>,
) {
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
    for message in rx.iter() {
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
}
