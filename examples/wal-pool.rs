#[cfg(feature = "wal-pool")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use nd_async_rusqlite::WalPool;

    const SETUP_SQL: &str = "
PRAGMA foreign_keys = ON; 
CREATE TABLE USERS (
    id INTEGER PRIMARY KEY, 
    first_name TEXT NOT NULL, 
    last_name TEXT NOT NULL
) STRICT;
";

    // Remember, don't use expect in real code.
    let connection = WalPool::builder()
        .readers(4)
        .writer_setup(|connection| Ok(connection.execute_batch(SETUP_SQL)?))
        .open("database.db")
        .await
        .expect("connection should be open");

    connection
        .close()
        .await
        .expect("an error occured while closing");
}

#[cfg(not(feature = "wal-pool"))]
fn main() {
    println!("Enable the \"wal-pool\" feature to run this example.");
}
