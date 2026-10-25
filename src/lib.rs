#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

//! # nd-async-rusqlite
//!
//! Utilities for accessing an Sqlite database via [`rusqlite`](https://docs.rs/rusqlite/latest/rusqlite/) in an async runtime.
//!
//! Blocking should not be done in async code.
//! However, Sqlite (and therefore `rusqlite`) blocks and cannot be used in async code.
//! This library offers a solution by creating a background thread for sqlite calls and communicating with it via channels, allowing rusqlite to be used in async code.
//!
//! This library also offers a connection pool via the `WalPool` type from the `wal-pool` feature.
//! This simple connection pool will maintain a user-specified number of readers and one writer, in order to make the most of `WAL` mode.
//!
//! ## Features
//!
//! | Name          | Description                                                               |
//! | ------------- | ------------------------------------------------------------------------- |
//! | array         | Enable rusqlite's `array` feature                                         |
//! | backup        | Enable rusqlite's `backup` feature                                        |
//! | bundled       | Enable rusqlite's `bundled` feature                                       |
//! | cache         | Enable rusqlite's `cache` feature                                         |
//! | fallible_uint | Enable rusqlite's `fallible_uint` feature                                 |
//! | functions     | Enable rusqlite's `function` feature                                      |
//! | time          | Enable rusqlite's `time` feature                                          |
//! | trace         | Enable rusqlite's `trace` feature                                         |
//! | url           | Enable rusqlite's `url` feature                                           |
//! | wal-pool      | Enable the `WalPool` type, a concurrent connection pool for WAL databases |

mod async_connection;
mod sync_wrapper;
#[cfg(feature = "wal-pool")]
mod wal_pool;

pub use self::async_connection::AsyncConnection;
pub use self::async_connection::AsyncConnectionBuilder;
pub use self::sync_wrapper::SyncWrapper;
#[cfg(feature = "wal-pool")]
pub use self::wal_pool::WalPool;
#[cfg(feature = "wal-pool")]
pub use self::wal_pool::WalPoolBuilder;
pub use rusqlite;

/// The library error type
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// A rusqlite error
    Rusqlite(rusqlite::Error),

    /// The request was aborted for some reason.
    ///
    /// This means that the background thread shutdown while this request was in-flight.
    Aborted,

    /// An access panicked.
    ///
    /// You can re-throw the panic data.
    /// The background thread is still alive.
    AccessPanic(SyncWrapper<Box<dyn std::any::Any + Send + 'static>>),

    /// The WalPool attempted to put the database into WAL mode, but failed.
    #[cfg(feature = "wal-pool")]
    InvalidJournalMode(String),

    /// A generic error.
    Generic(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rusqlite(error) => error.fmt(f),
            Self::Aborted => "The connection thread aborted the request".fmt(f),
            Self::AccessPanic(_) => "A connection access panicked".fmt(f),
            Self::Generic(message) => message.fmt(f),

            #[cfg(feature = "wal-pool")]
            Self::InvalidJournalMode(journal_mode) => {
                write!(
                    f,
                    "Failed to set journal_mode to WAL, journal_mode is \"{journal_mode}\""
                )
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rusqlite(error) => Some(error),
            Self::Aborted => None,
            Self::AccessPanic(_) => None,
            Self::Generic(_) => None,

            #[cfg(feature = "wal-pool")]
            Self::InvalidJournalMode(_) => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Rusqlite(error)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    pub const fn _assert_send<T>()
    where
        T: Send,
    {
    }
    pub const fn _assert_sync<T>()
    where
        T: Sync,
    {
    }
    pub const fn _assert_static_lifetime<T>()
    where
        T: 'static,
    {
    }

    const _ERROR_IS_SEND: () = _assert_send::<Error>();
    const _ERROR_IS_SYNC: () = _assert_sync::<Error>();
    const _ERROR_HAS_A_STATIC_LIFETIME: () = _assert_static_lifetime::<Error>();

    #[tokio::test]
    async fn sanity() {
        let temp_path = Path::new("test-temp");
        std::fs::create_dir_all(temp_path).expect("Failed to create temp dir");

        let connection_error = AsyncConnection::builder()
            .open(".")
            .await
            .expect_err("Connection should not open on a directory");
        assert!(matches!(connection_error, Error::Rusqlite(_)));

        let connection_path = temp_path.join("sanity.db");
        match std::fs::remove_file(&connection_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                panic!("Failed to remove old database: {error:?}");
            }
        }

        let connection = AsyncConnection::builder()
            .open(connection_path)
            .await
            .expect("Connection should be open");

        // Ensure connection is clone
        let _connection1 = connection.clone();

        // Ensure connection survives panic
        let panic_error = connection
            .access(|_connection| panic!("The connection should survive the panic"))
            .await
            .expect_err("The access should have failed");

        assert!(matches!(panic_error, Error::AccessPanic(_)));

        let setup_sql = "PRAGMA foreign_keys = ON; CREATE TABLE USERS (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL) STRICT;";
        connection
            .access(|connection| connection.execute_batch(setup_sql))
            .await
            .expect("Failed to create tables")
            .expect("Failed to execute");

        connection
            .close()
            .await
            .expect("An error occured while closing");
    }
}
