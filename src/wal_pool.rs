use crate::Error;
use crate::SyncWrapper;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// The channel message
enum Message {
    Access {
        func: Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>,
    },
    Close {
        tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

/// A handle to a pool of connections, designed for a database in WAL mode.
#[derive(Debug, Clone)]
pub struct WalPool {
    inner: Arc<InnerWalPool>,
}

impl WalPool {
    /// Get a builder for a [`WalPool`].
    pub fn builder() -> WalPoolBuilder {
        WalPoolBuilder::new()
    }

    /// Get a write permit, if needed.
    async fn get_write_permit(&self) -> Option<tokio::sync::SemaphorePermit> {
        // TODO: How should a no-permit situation be handled?
        // Should we return an error to the caller, or simply wait?
        //
        // The benefit of waiting is that the case where a loop creates requests without awaiting them can be limitied by waiting, here.
        // The benefit of returning an error is handling high-load situations by returning errors to requests that cannot be fufilled in a timely manner.
        match self.inner.write_semaphore.as_ref() {
            Some(semaphore) => {
                // We never close the semaphore.
                Some(semaphore.acquire().await.unwrap())
            }
            None => None,
        }
    }

    /// Get a read permit, if needed.
    async fn get_read_permit(&self) -> Option<tokio::sync::SemaphorePermit> {
        // TODO: How should a no-permit situation be handled?
        // Should we return an error to the caller, or simply wait?
        //
        // The benefit of waiting is that the case where a loop creates requests without awaiting them can be limitied by waiting, here.
        // The benefit of returning an error is handling high-load situations by returning errors to requests that cannot be fufilled in a timely manner.
        match self.inner.read_semaphore.as_ref() {
            Some(semaphore) => {
                // We never close the semaphore.
                Some(semaphore.acquire().await.unwrap())
            }
            None => None,
        }
    }

    /// Close the pool.
    ///
    /// This will queue a close request to each thread.
    /// When each thread processes the close request,
    /// it will shut down.
    /// When the last thread processed the close message,
    /// all current queued requests will be aborted.
    ///
    /// When this function returns,
    /// the pool will be closed no matter the value of the return.
    /// The return value will return the last error that occured while closing.
    pub async fn close(&self) -> Result<(), Error> {
        let mut last_error = Ok(());

        // Close the writer
        let close_writer_result = async {
            let permit = self.get_write_permit().await;
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.inner
                .writer_tx
                .send(Message::Close { tx })
                .map_err(|_| Error::Aborted)?;
            rx.await.map_err(|_| Error::Aborted)??;
            drop(permit);

            Ok(())
        }
        .await;

        if let Err(close_writer_error) = close_writer_result {
            last_error = Err(close_writer_error);
        }

        loop {
            let permit = self.get_write_permit().await;
            let (tx, rx) = tokio::sync::oneshot::channel();
            let send_result = self.inner.readers_tx.send(Message::Close { tx });

            if let Err(_error) = send_result {
                // All receivers closed, we can stop now.
                break;
            }

            let close_result = rx
                .await
                .map_err(|_| Error::Aborted)
                .and_then(std::convert::identity);

            if let Err(close_error) = close_result {
                last_error = Err(close_error);
            }

            drop(permit);
        }

        last_error
    }

    /// Access the database with a reader connection.
    ///
    /// Note that dropping the returned future will not cancel the database access.
    pub async fn read<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        // TODO: We should make this a function and have it return a named Future.
        // This will allow users to avoid spawning a seperate task for each database call.

        let permit = self.get_read_permit().await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner
            .readers_tx
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
        drop(permit);

        Ok(result)
    }

    /// Access the database with a writer connection.
    ///
    /// Note that dropping the returned future will not cancel the database access.
    pub async fn write<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        // TODO: We should make this a function and have it return a named Future.
        // This will allow users to avoid spawning a seperate task for each database call.

        let permit = self.get_write_permit().await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner
            .writer_tx
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
        drop(permit);

        Ok(result)
    }
}

/// The inner wal pool
#[derive(Debug)]
struct InnerWalPool {
    writer_tx: crossbeam_channel::Sender<Message>,
    readers_tx: crossbeam_channel::Sender<Message>,

    write_semaphore: Option<Semaphore>,
    read_semaphore: Option<Semaphore>,
}

/// A builder for a [`WalPool`].
pub struct WalPoolBuilder {
    /// The maximum number of reads that may be queued.
    pub max_queued_reads: Option<usize>,

    /// The maximum number of writes that may be queued.
    pub max_queued_writes: Option<usize>,

    /// The number of read connections
    pub num_read_connections: usize,
}

impl WalPoolBuilder {
    /// Make a new [`WalPoolBuilder`].
    pub fn new() -> Self {
        // TODO: Try to find some sane defaults experimentally.
        Self {
            max_queued_reads: Some(128),
            max_queued_writes: Some(32),

            num_read_connections: 4,
        }
    }

    /// Set the maximum number of queued reads.
    pub fn max_queued_reads(&mut self, max_queued_reads: Option<usize>) -> &mut Self {
        self.max_queued_reads = max_queued_reads;
        self
    }

    /// Set the maximum number of queued writes.
    pub fn max_queued_writes(&mut self, max_queued_writes: Option<usize>) -> &mut Self {
        self.max_queued_writes = max_queued_writes;
        self
    }

    /// Set the number of read connections.
    pub fn num_read_connections(&mut self, num_read_connections: usize) -> &mut Self {
        self.num_read_connections = num_read_connections;
        self
    }

    /// Open the pool.
    pub async fn open<P>(&self, path: P) -> Result<WalPool, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        // Do this first, this can panic if values are too large.
        let read_semaphore = self.max_queued_reads.map(Semaphore::new);
        let write_semaphore = self.max_queued_writes.map(Semaphore::new);

        // Bring connections up all at once for speed.
        let (writer_tx, writer_rx) = crossbeam_channel::unbounded::<Message>();
        let (open_write_tx, open_write_rx) = tokio::sync::oneshot::channel();
        {
            let path = path.clone();
            std::thread::spawn(move || {
                write_connection_thread_impl(writer_rx, path, open_write_tx)
            });
        }
        let (readers_tx, readers_rx) = crossbeam_channel::unbounded::<Message>();
        let mut open_read_rx_list = Vec::with_capacity(self.num_read_connections);
        for _ in 0..self.num_read_connections {
            let readers_rx = readers_rx.clone();
            let path = path.clone();
            let (open_read_tx, open_read_rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                write_connection_thread_impl(readers_rx, path.clone(), open_read_tx)
            });
            open_read_rx_list.push(open_read_rx);
        }
        drop(readers_rx);

        // Create the wal pool here.
        // This lets us at least attempt to close it later.
        let wal_pool = WalPool {
            inner: Arc::new(InnerWalPool {
                writer_tx,
                readers_tx,

                write_semaphore,
                read_semaphore,
            }),
        };

        let mut last_error = Ok(());
        if let Err(error) = open_write_rx
            .await
            .map_err(|_| Error::Aborted)
            .and_then(|result| result.map_err(Error::Rusqlite))
        {
            last_error = Err(error);
        }

        for open_read_rx in open_read_rx_list {
            if let Err(error) = open_read_rx
                .await
                .map_err(|_| Error::Aborted)
                .and_then(|result| result.map_err(Error::Rusqlite))
            {
                last_error = Err(error);
            }
        }

        if let Err(error) = last_error {
            // At least try to bing it down nicely.
            // We ignore the error, since the original error is much more important.
            let _ = wal_pool.close().await.is_ok();

            return Err(error);
        }

        Ok(wal_pool)
    }
}

impl Default for WalPoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The impl for the connection background thread.
fn write_connection_thread_impl(
    rx: crossbeam_channel::Receiver<Message>,
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

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn sanity() {
        let temp_path = Path::new("test-temp");
        std::fs::create_dir_all(temp_path).expect("failed to create temp dir");

        let connection_error = WalPool::builder()
            .open(".")
            .await
            .expect_err("pool should not open on a directory");
        assert!(matches!(connection_error, Error::Rusqlite(_)));

        let connection_path = temp_path.join("wal-pool-sanity.db");
        match std::fs::remove_file(&connection_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                panic!("failed to remove old database: {error:?}");
            }
        }

        let connection = WalPool::builder()
            .open(connection_path)
            .await
            .expect("connection should be open");

        // Ensure connection is clone
        let _connection1 = connection.clone();

        // Ensure write connection survives panic
        let panic_error = connection
            .write(|_connection| panic!("the connection should survive the panic"))
            .await
            .expect_err("the access should have failed");

        assert!(matches!(panic_error, Error::AccessPanic(_)));

        let setup_sql = "PRAGMA foreign_keys = ON; CREATE TABLE USERS (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL) STRICT;";
        connection
            .write(|connection| connection.execute_batch(setup_sql))
            .await
            .expect("failed to create tables")
            .expect("failed to execute");

        connection
            .close()
            .await
            .expect("an error occured while closing");
    }
}
