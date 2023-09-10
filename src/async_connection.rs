mod builder;

pub use self::builder::AsyncConnectionBuilder;
use crate::Error;
use crate::SyncWrapper;

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
