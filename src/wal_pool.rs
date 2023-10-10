mod builder;

pub use self::builder::WalPoolBuilder;
use crate::Error;
use crate::SyncWrapper;
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

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::path::Path;

    #[tokio::test]
    async fn dir() {
        let connection_error = WalPool::builder()
            .open(".")
            .await
            .expect_err("pool should not open on a directory");
        assert!(matches!(connection_error, Error::Rusqlite(_)));
    }

    #[tokio::test]
    async fn sanity() {
        let temp_path = Path::new("test-temp");
        std::fs::create_dir_all(temp_path).expect("failed to create temp dir");

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
                .open(&connection_path)
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

    #[tokio::test]
    async fn init_fn_panics() {
        let temp_path = Path::new("test-temp");
        let connection_path = temp_path.join("wal-pool-init_fn_panics.db");

        WalPool::builder()
            .writer_init_fn(move |_connection| {
                panic!("user panic");
            })
            .open(&connection_path)
            .await
            .expect_err("panic should become an error");

        WalPool::builder()
            .reader_init_fn(move |_connection| {
                panic!("user panic");
            })
            .open(&connection_path)
            .await
            .expect_err("panic should become an error");
    }
}
