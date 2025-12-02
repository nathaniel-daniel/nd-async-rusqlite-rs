# nd-async-rusqlite
Utilities for accessing an sqlite database via [rusqlite](https://docs.rs/rusqlite/latest/rusqlite/) in an async runtime.

Blocking should not be done in async code.
However, sqlite (and therefore rusqlite) blocks and cannot be used in async code.
This library offers a solution by creating a background thread for sqlite calls and communicating with it via channels, allowing rusqlite to be used in async code.

## Examples

### Simple
```rust
use nd_async_rusqlite::AsyncConnection;
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
        .expect("failed to access database")
        .expect("failed to setup sql");

    connection
        .access(|connection| {
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

    let (id, first_name, last_name) = connection
        .access(|connection| {
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

    connection
        .close()
        .await
        .expect("an error occured while closing");
}
```

### WalPool
```rust
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
```

## Features
| Name      | Description                                                             |
|-----------|-------------------------------------------------------------------------|
| backup    | Enable rusqlite's backup feature                                        |
| bundled   | Enable rusqlite's bundled feature                                       |
| functions | Enable rusqlite's function feature                                      |
| time      | Enable rusqlite's time feature                                          |
| trace     | Enable rusqlite's trace feature                                         |
| url       | Enable rusqlite's url feature                                           |
| wal-pool  | Enable the WalPool type, a concurrent connection pool for WAL databases | 

## License
Licensed under either of
 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.