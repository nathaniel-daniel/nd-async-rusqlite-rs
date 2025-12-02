#[cfg(feature = "wal-pool")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use nd_async_rusqlite::WalPool;
    use rusqlite::named_params;

    const SETUP_SQL: &str = "
PRAGMA foreign_keys = ON; 
CREATE TABLE user (
    id INTEGER PRIMARY KEY, 
    first_name TEXT NOT NULL, 
    last_name TEXT NOT NULL
) STRICT;
";

    const INSERT_USER_SQL: &str = "
INSERT INTO user (
    first_name,
    last_name
) VALUES (
    :first_name,
    :last_name
);
";

    const GET_USER_BY_FIRST_NAME_SQL: &str = "
SELECT
    id,
    first_name,
    last_name
FROM
    user
WHERE
    first_name = :first_name;
";

    // Remember, don't use expect in real code.
    let pool = WalPool::builder()
        .readers(4)
        .writer_setup(|connection| {
            println!("Writer connection starting up!");
            connection.execute_batch(SETUP_SQL)?;
            Ok(())
        })
        .reader_setup(|_connection| {
            println!("Reader connection starting up!");
            Ok(())
        })
        .open("database.db")
        .await
        .expect("connection should be open");

    pool.write(|connection| {
        connection.execute(
            INSERT_USER_SQL,
            named_params! {
                ":first_name": "John",
                ":last_name": "Doe",
            },
        )
    })
    .await
    .expect("failed to access database")
    .expect("failed to insert row");

    let (id, first_name, last_name) = pool
        .read(|connection| {
            connection.query_one(
                GET_USER_BY_FIRST_NAME_SQL,
                named_params! {
                    ":first_name": "John",
                },
                |row| {
                    let id: i64 = row.get("id")?;
                    let first_name: String = row.get("first_name")?;
                    let last_name: String = row.get("last_name")?;

                    Ok((id, first_name, last_name))
                },
            )
        })
        .await
        .expect("failed to access database")
        .expect("failed to get row");

    println!("Id: {id}, first_name: {first_name}, last_name: {last_name}");

    pool.close().await.expect("an error occured while closing");
}

#[cfg(not(feature = "wal-pool"))]
fn main() {
    println!("Enable the \"wal-pool\" feature to run this example.");
}
