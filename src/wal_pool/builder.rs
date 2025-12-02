use super::InnerWalPool;
use super::Message;
use super::WalPool;
use crate::Error;
use crate::SyncWrapper;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

const DEFAULT_READERS: usize = 4;

type ConnectionSetupFn =
    Arc<dyn Fn(&mut rusqlite::Connection) -> Result<(), Error> + Send + Sync + 'static>;

/// A builder for a [`WalPool`].
pub struct WalPoolBuilder {
    /// The number of read connections
    pub readers: usize,

    /// A function to be called to initialize each reader.
    pub reader_setup: Option<ConnectionSetupFn>,

    /// A function to be called to initialize the writer.
    pub writer_setup: Option<ConnectionSetupFn>,
}

impl WalPoolBuilder {
    /// Make a new [`WalPoolBuilder`].
    pub fn new() -> Self {
        // TODO: Try to find some sane defaults experimentally.
        Self {
            readers: DEFAULT_READERS,

            reader_setup: None,
            writer_setup: None,
        }
    }

    /// Set the number of read connections.
    ///
    /// This must be greater than 0.
    pub fn readers(&mut self, readers: usize) -> &mut Self {
        self.readers = readers;
        self
    }

    /// Add a function to be called when a reader connection initializes.
    pub fn reader_setup<F>(&mut self, reader_setup: F) -> &mut Self
    where
        F: Fn(&mut rusqlite::Connection) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.reader_setup = Some(Arc::new(reader_setup));
        self
    }

    /// Add a function to be called when the writer connection initializes.
    pub fn writer_setup<F>(&mut self, writer_setup: F) -> &mut Self
    where
        F: Fn(&mut rusqlite::Connection) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.writer_setup = Some(Arc::new(writer_setup));
        self
    }

    /// Open the pool.
    pub async fn open<P>(&self, path: P) -> Result<WalPool, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        let readers = self.readers;
        if self.readers == 0 {
            return Err(Error::Generic("`readers` cannot be 0"));
        }

        // Only the writer can create the database, make sure it does so before doing anything else.
        let writer_tx = {
            let (writer_tx, writer_rx) = crossbeam_channel::unbounded::<Message>();
            let path = path.clone();
            let flags = rusqlite::OpenFlags::default();
            let writer_setup = self.writer_setup.clone();
            let (open_write_tx, open_write_rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                connection_thread_impl(writer_rx, path, flags, writer_setup, open_write_tx)
            });

            open_write_rx
                .await
                .map_err(|_| Error::Aborted)
                .and_then(std::convert::identity)?;

            writer_tx
        };

        // Bring reader connections up all at once for speed.
        let (readers_tx, readers_rx) = crossbeam_channel::unbounded::<Message>();
        let mut open_read_rx_list = Vec::with_capacity(readers);
        for _ in 0..readers {
            let readers_rx = readers_rx.clone();
            let path = path.clone();

            // We cannot allow writing in reader connections, forcibly set bits.
            let mut flags = rusqlite::OpenFlags::default();
            flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE);
            flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);
            flags.insert(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);

            let reader_setup = self.reader_setup.clone();

            let (open_read_tx, open_read_rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                connection_thread_impl(readers_rx, path, flags, reader_setup, open_read_tx)
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
            // At least try to bring it down nicely.
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

/// Set the journal_mode to WAL.
///
/// # References
/// * https://www.sqlite.org/wal.html#activating_and_configuring_wal_mode
fn set_wal_journal_mode(connection: &rusqlite::Connection) -> Result<(), Error> {
    let journal_mode: String =
        connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;

    if journal_mode != "wal" {
        return Err(Error::InvalidJournalMode(journal_mode));
    }

    Ok(())
}

/// The impl for the connection background thread.
fn connection_thread_impl(
    rx: crossbeam_channel::Receiver<Message>,
    path: PathBuf,
    flags: rusqlite::OpenFlags,
    init_fn: Option<ConnectionSetupFn>,
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
    if let Err(error) = set_wal_journal_mode(&connection) {
        // Don't care if we succed since we should exit in either case.
        let _ = connection_open_tx.send(Err(error)).is_ok();
        return;
    }

    // Run init fn.
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
