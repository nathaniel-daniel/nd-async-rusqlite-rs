use nd_async_rusqlite::AsyncConnection;

const SETUP_SQL: &str = "
PRAGMA foreign_keys = ON; 
CREATE TABLE USERS (
    id INTEGER PRIMARY KEY, 
    first_name TEXT NOT NULL, 
    last_name TEXT NOT NULL
) STRICT;
";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Remember, don't use expect in real code.
    let connection = AsyncConnection::builder()
        .open("database.db")
        .await
        .expect("connection should be open");

    connection
        .access(|connection| connection.execute_batch(SETUP_SQL))
        .await
        .expect("failed to create tables")
        .expect("failed to execute");

    connection
        .close()
        .await
        .expect("an error occured while closing");
}
