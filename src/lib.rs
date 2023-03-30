mod async_connection;
mod sync_wrapper;

pub use self::async_connection::AsyncConnection;
pub use self::async_connection::AsyncConnectionBuilder;
pub use self::sync_wrapper::SyncWrapper;
pub use rusqlite;

/// The library error type
#[derive(Debug)]
pub enum Error {
    /// A rusqlite error
    Rusqlite(rusqlite::Error),

    /// The request was aborted
    Aborted,

    /// An access panicked
    AccessPanic(SyncWrapper<Box<dyn std::any::Any + Send + 'static>>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rusqlite(error) => error.fmt(f),
            Self::Aborted => "the connection thread aborted the request".fmt(f),
            Self::AccessPanic(_) => "a connection access panicked".fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rusqlite(error) => Some(error),
            Self::Aborted => None,
            Self::AccessPanic(_) => None,
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

    const fn _assert_send<T>()
    where
        T: Send,
    {
    }
    const fn _assert_sync<T>()
    where
        T: Sync,
    {
    }

    const fn _assert_static_lifetime<T>()
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
        std::fs::create_dir_all(temp_path).expect("failed to create temp dir");

        let connection_error = AsyncConnection::builder()
            .message_channel_capacity(128)
            .open(".")
            .await
            .expect_err("connection should not open on a directory");
        assert!(matches!(connection_error, Error::Rusqlite(_)));

        let connection_path = temp_path.join("sanity.db");
        match std::fs::remove_file(&connection_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                Result::<(), std::io::Error>::Err(e).expect("failed to remove old database");
            }
        }

        // Ensure connection can be opened and used from a blocking context
        {
            let connection_path = connection_path.clone();
            tokio::task::spawn_blocking(|| {
                let connection = AsyncConnection::builder()
                    .blocking_open(connection_path)
                    .expect("connection should be open");
                connection
                    .blocking_access(|connection| {
                        connection.execute(
                            "CREATE TABLE blocking (id INTEGER NOT NULL PRIMARY KEY) STRICT;",
                            [],
                        )
                    })
                    .expect("failed to run blocking access")
                    .expect("query failed");
            })
            .await
            .expect("failed to join");
        }

        let connection = AsyncConnection::builder()
            .open(connection_path)
            .await
            .expect("connection should be open");

        // Ensure connection is clone
        let _connection1 = connection.clone();

        // Ensure connection survives panic
        let panic_error = connection
            .access(|_connection| panic!("the connection should survive the panic"))
            .await
            .expect_err("the access should have failed");

        assert!(matches!(panic_error, Error::AccessPanic(_)));

        let setup_sql = "PRAGMA foreign_keys = ON; CREATE TABLE USERS (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL) STRICT;";
        connection
            .access(|connection| connection.execute_batch(setup_sql))
            .await
            .expect("failed to create tables")
            .expect("failed to execute");

        connection
            .close()
            .await
            .expect("an error occured while closing");
    }
}
