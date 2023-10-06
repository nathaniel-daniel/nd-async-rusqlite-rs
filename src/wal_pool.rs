use crate::Error;
use crate::SyncWrapper;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

type ConnectionInitFn =
    Arc<dyn Fn(&mut rusqlite::Connection) -> Result<(), Error> + Send + Sync + 'static>;

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

    /// A function to be called to initialize each reader.
    pub reader_init_fn: Option<ConnectionInitFn>,

    /// A function to be called to initialize the writer.
    pub writer_init_fn: Option<ConnectionInitFn>,
}

impl WalPoolBuilder {
    /// Make a new [`WalPoolBuilder`].
    pub fn new() -> Self {
        // TODO: Try to find some sane defaults experimentally.
        Self {
            max_queued_reads: Some(128),
            max_queued_writes: Some(32),

            num_read_connections: 4,

            writer_init_fn: None,
            reader_init_fn: None,
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

    /// Add a function to be called when the writer connection initializes.
    pub fn writer_init_fn<F>(&mut self, writer_init_fn: F) -> &mut Self
    where
        F: Fn(&mut rusqlite::Connection) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.writer_init_fn = Some(Arc::new(writer_init_fn));
        self
    }

    /// Add a function to be called when a reader connection initializes.
    pub fn reader_init_fn<F>(&mut self, reader_init_fn: F) -> &mut Self
    where
        F: Fn(&mut rusqlite::Connection) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.reader_init_fn = Some(Arc::new(reader_init_fn));
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

        // Only the writer can create the database, make sure it works before doing anything else.
        let writer_tx = {
            let (writer_tx, writer_rx) = crossbeam_channel::unbounded::<Message>();
            let path = path.clone();
            let flags = rusqlite::OpenFlags::default();
            let writer_init_fn = self.writer_init_fn.clone();
            let (open_write_tx, open_write_rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                connection_thread_impl(writer_rx, path, flags, writer_init_fn, open_write_tx)
            });

            open_write_rx
                .await
                .map_err(|_| Error::Aborted)
                .and_then(std::convert::identity)?;

            writer_tx
        };

        // Bring reader connections up all at once for speed.
        let (readers_tx, readers_rx) = crossbeam_channel::unbounded::<Message>();
        let mut open_read_rx_list = Vec::with_capacity(self.num_read_connections);
        for _ in 0..self.num_read_connections {
            let readers_rx = readers_rx.clone();
            let path = path.clone();

            // We cannot allow writing in reader connections, forcibly set bits.
            let mut flags = rusqlite::OpenFlags::default();
            flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE);
            flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);
            flags.insert(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);

            let reader_init_fn = self.reader_init_fn.clone();

            let (open_read_tx, open_read_rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                connection_thread_impl(readers_rx, path, flags, reader_init_fn, open_read_tx)
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

        for open_read_rx in open_read_rx_list {
            if let Err(error) = open_read_rx
                .await
                .map_err(|_| Error::Aborted)
                .and_then(std::convert::identity)
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
fn connection_thread_impl(
    rx: crossbeam_channel::Receiver<Message>,
    path: PathBuf,
    flags: rusqlite::OpenFlags,
    init_fn: Option<ConnectionInitFn>,
    connection_open_tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
) {
    // Open the database, reporting errors as necessary.
    let open_result = rusqlite::Connection::open_with_flags(path, flags);
    let mut connection = match open_result {
        Ok(connection) => connection,
        Err(error) => {
            // Don't care if we succed since we should exit in either case.
            let _ = connection_open_tx.send(Err(Error::Rusqlite(error))).is_ok();
            return;
        }
    };

    // If WAL mode fails to enable, we should exit.
    // This abstraction is fairly worthless outside of WAL mode.
    {
        let journal_mode_result =
            connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| {
                row.get::<_, String>(0)
            });

        let journal_mode = match journal_mode_result {
            Ok(journal_mode) => journal_mode,
            Err(error) => {
                // Don't care if we succed since we should exit in either case.
                let _ = connection_open_tx.send(Err(Error::Rusqlite(error))).is_ok();
                return;
            }
        };

        if journal_mode != "wal" {
            // Don't care if we succed since we should exit in either case.
            let _ = connection_open_tx
                .send(Err(Error::InvalidJournalMode(journal_mode)))
                .is_ok();
            return;
        }
    }

    if let Some(init_fn) = init_fn {
        let init_fn = std::panic::AssertUnwindSafe(|| init_fn(&mut connection));
        let init_result = std::panic::catch_unwind(init_fn);
        let init_result =
            init_result.map_err(|panic_data| Error::AccessPanic(SyncWrapper::new(panic_data)));
        if let Err(error) = init_result {
            // Don't care if we succeed since we should exit in either case.
            let _ = connection_open_tx.send(Err(error)).is_ok();
            return;
        }
    }

    // Check if the user cancelled the opening of the database connection and return early if needed.
    if connection_open_tx.send(Ok(())).is_err() {
        return;
    }

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
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

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

        let writer_init_fn_called = Arc::new(AtomicBool::new(false));
        let num_reader_init_fn_called = Arc::new(AtomicUsize::new(0));
        let num_read_connections = 4;
        let connection = {
            let writer_init_fn_called = writer_init_fn_called.clone();
            let num_reader_init_fn_called = num_reader_init_fn_called.clone();
            WalPool::builder()
                .num_read_connections(num_read_connections)
                .writer_init_fn(move |_connection| {
                    writer_init_fn_called.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .reader_init_fn(move |_connection| {
                    num_reader_init_fn_called.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .open(connection_path)
                .await
                .expect("connection should be open")
        };
        let writer_init_fn_called = writer_init_fn_called.load(Ordering::SeqCst);
        let num_reader_init_fn_called = num_reader_init_fn_called.load(Ordering::SeqCst);

        assert!(writer_init_fn_called);
        assert!(num_reader_init_fn_called == num_read_connections);

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

        // Reader should not be able to write
        connection
            .read(|connection| connection.execute_batch(setup_sql))
            .await
            .expect("failed to access")
            .expect_err("write should have failed");

        connection
            .close()
            .await
            .expect("an error occured while closing");
    }
}
