mod builder;

pub use self::builder::WalPoolBuilder;
use crate::Error;
use crate::SyncWrapper;
use std::path::Path;
use std::sync::Arc;

/// The channel message
enum Message {
    Access {
        func: Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>,
    },
    Close {
        tx: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

/// An asynchronous interface to a pool of SQLite connections on a thread pool, designed for WAL mode.
///
/// `WalPool` executes database operations on a background thead pool.
/// The pool consistsof 1 writer thread and a user-configurable number of reader threads,
/// allowing async code to use [`rusqlite`] without blocking.
///
/// # Examples
/// ```no_run
/// use nd_async_rusqlite::WalPool;
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = WalPool::builder()
///        .readers(4)
///        .writer_setup(|connection| {
///            println!("Writer connection starting up!");
///            // This is a good place to set up your tables.
///            Ok(())
///        })
///        .reader_setup(|_connection| {
///            println!("Reader connection starting up!");
///            Ok(())
///        })
///        .open("database.db")
///        .await
///        .expect("Connection should be open");
///
///     pool
///         .write(|conn| {
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
///    pool.close().await?;
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct WalPool {
    inner: Arc<InnerWalPool>,
}

impl WalPool {
    /// Open the pool.
    ///
    /// This will automatically put the pool into WAL mode.
    /// If you want to configure per-connection settings or configure the number of readers, use [`WalPool::builder`].
    pub async fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        Self::builder().open(path.as_ref()).await
    }

    /// Get a builder for a [`WalPool`].
    pub fn builder() -> WalPoolBuilder {
        WalPoolBuilder::new()
    }

    /// Executes a closure with read access to the database.
    ///
    /// The closure is executed on the background thread pool. The
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
    /// # use nd_async_rusqlite::WalPool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// #     let pool = WalPool::open("example.db").await?;
    ///
    ///     pool
    ///         .read(|conn| {
    ///             let mut statement = conn.prepare("SELECT name FROM users;")?;
    ///             let names: Vec<String> = statement
    ///                 .query_map(
    ///                     [],
    ///                     |row| {
    ///                         let name: String = row.get(0)?;
    ///                         Ok(name)
    ///                     },
    ///                 )?
    ///                 .collect::<Result<_, _>>()?;
    ///             Ok::<_, rusqlite::Error>(names)
    ///         })
    ///         .await??;
    ///
    /// #     Ok(())
    /// # }
    /// ```
    pub async fn read<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        // TODO: We should make this a function and have it return a named Future.
        // This will allow users to avoid spawning a seperate task for each database call.

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

        Ok(result)
    }

    /// Executes a closure with write access to the database.
    ///
    /// The closure is executed on the background thread pool. The
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
    /// # use nd_async_rusqlite::WalPool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// #     let pool = WalPool::open("example.db").await?;
    ///
    ///     pool
    ///         .write(|conn| {
    ///             let mut statement = conn.prepare("INSERT INTO users (?1, ?2);")?;
    ///             statement.execute((24, "John"))?;
    ///             Ok::<_, rusqlite::Error>(())
    ///         })
    ///         .await??;
    ///
    /// #     Ok(())
    /// # }
    /// ```
    pub async fn write<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut rusqlite::Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        // TODO: We should make this a function and have it return a named Future.
        // This will allow users to avoid spawning a seperate task for each database call.

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
        Ok(result)
    }

    /// Close the pool.
    ///
    /// This will queue a close request to each thread.
    /// When each thread processes the close request, it will shut down.
    /// When the last thread processes the close message,
    /// all current in-flight requests will be aborted.
    ///
    /// When this function returns,
    /// the pool will be closed no matter the value of the return.
    /// The return value will return the last error that occured while closing.
    pub async fn close(&self) -> Result<(), Error> {
        let mut last_error = Ok(());

        loop {
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
        }

        // Close the writer
        let close_writer_result = async {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.inner
                .writer_tx
                .send(Message::Close { tx })
                .map_err(|_| Error::Aborted)?;
            rx.await.map_err(|_| Error::Aborted)??;
            Ok(())
        }
        .await;

        if let Err(close_writer_error) = close_writer_result {
            last_error = Err(close_writer_error);
        }

        last_error
    }
}

/// The inner WAL pool.
#[derive(Debug)]
struct InnerWalPool {
    writer_tx: crossbeam_channel::Sender<Message>,
    readers_tx: crossbeam_channel::Sender<Message>,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn dir() {
        let connection_error = WalPool::builder()
            .open(".")
            .await
            .expect_err("Pool should not open on a directory");
        assert!(matches!(connection_error, Error::Rusqlite(_)));
    }

    #[tokio::test]
    async fn sanity() {
        let temp_path = Path::new("test-temp");
        std::fs::create_dir_all(temp_path).expect("Failed to create temp dir");

        let connection_path = temp_path.join("wal-pool-sanity.db");
        match std::fs::remove_file(&connection_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                panic!("Failed to remove old database: {error:?}");
            }
        }

        let writer_init_fn_called = Arc::new(AtomicBool::new(false));
        let num_reader_init_fn_called = Arc::new(AtomicUsize::new(0));
        let readers = 4;
        let connection = {
            let writer_init_fn_called = writer_init_fn_called.clone();
            let num_reader_init_fn_called = num_reader_init_fn_called.clone();
            WalPool::builder()
                .readers(readers)
                .writer_setup(move |_connection| {
                    writer_init_fn_called.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .reader_setup(move |_connection| {
                    num_reader_init_fn_called.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .open(&connection_path)
                .await
                .expect("Connection should be open")
        };
        let writer_init_fn_called = writer_init_fn_called.load(Ordering::SeqCst);
        let num_reader_init_fn_called = num_reader_init_fn_called.load(Ordering::SeqCst);

        assert!(writer_init_fn_called);
        assert!(num_reader_init_fn_called == readers);

        // Ensure connection is clone
        let _connection1 = connection.clone();

        // Ensure write connection survives panic
        let panic_error = connection
            .write(|_connection| panic!("The connection should survive the panic"))
            .await
            .expect_err("The access should have failed");

        assert!(matches!(panic_error, Error::AccessPanic(_)));

        let setup_sql = "PRAGMA foreign_keys = ON; CREATE TABLE USERS (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL) STRICT;";
        connection
            .write(|connection| connection.execute_batch(setup_sql))
            .await
            .expect("Failed to create tables")
            .expect("Failed to execute");

        // Reader should not be able to write
        connection
            .read(|connection| connection.execute_batch(setup_sql))
            .await
            .expect("Failed to access")
            .expect_err("Write should have failed");

        connection
            .close()
            .await
            .expect("An error occured while closing");
    }

    #[tokio::test]
    async fn init_fn_panics() {
        let temp_path = Path::new("test-temp");
        let connection_path = temp_path.join("wal-pool-init_fn_panics.db");

        WalPool::builder()
            .writer_setup(move |_connection| {
                panic!("User panic");
            })
            .open(&connection_path)
            .await
            .expect_err("Panic should become an error");

        WalPool::builder()
            .reader_setup(move |_connection| {
                panic!("User panic");
            })
            .open(&connection_path)
            .await
            .expect_err("Panic should become an error");
    }
}
