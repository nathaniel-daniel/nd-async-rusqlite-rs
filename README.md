# nd-async-rusqlite
Utilities for accessing an sqlite database via [rusqlite](https://docs.rs/rusqlite/latest/rusqlite/) in an async runtime.

Blocking should not be done in async code.
However, sqlite (and therefore rusqlite) blocks and cannot be used in async code.
This library offers a solution by creating a background thread for sqlite calls and communicating with it via channels, allowing rusqlite to be used in async code.

## Example
```rust
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
        
    // Access the connection with the access method.
    // The return value of the closure is the return value of this future.
    connection
        .access(|connection| connection.execute_batch(SETUP_SQL))
        .await
        .expect("failed to create tables")
        .expect("failed to execute");
      
    // Close the database.
    // This queues a close message.
    // Messages queued before it will still complete.
    // When the close message is processed, all other messages are cancelled.
    connection
        .close()
        .await
        .expect("an error occured while closing");
}
```

## Features
| Name    | Description                       |
|---------|-----------------------------------|
| backup  | Enable rusqlite's backup feature  |
| bundled | Enable rusqlite's bundled feature |
| time    | Enable rusqlite's time feature    |
| trace   | Enable rusqlite's trace feature   |
| url     | Enable rusqlite's url feature     |

## License
Licensed under either of
 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.