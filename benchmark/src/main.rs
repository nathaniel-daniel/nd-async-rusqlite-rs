mod nd_async_rusqlite_impl;
mod rusqlite_mutex_impl;
mod tokio_rusqlite_impl;

use anyhow::Context;
use async_trait::async_trait;

use nd_async_rusqlite::rusqlite;
use rand::RngExt;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::StdRng;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinSet;

const NAMES_TXT: &str = include_str!("./names.txt");
static NAMES: LazyLock<Vec<&str>> = LazyLock::new(|| {
    NAMES_TXT
        .split('\n')
        .filter(|value| !value.is_empty())
        .collect()
});

const OPERATIONS: u64 = 100_000;
const DATABASE_PATH: &str = "database.db";

const SET_WAL_MODE_SQL: &str = "PRAGMA journal_mode=WAL;";
const SETUP_SQL: &[&str] = &[
    "PRAGMA synchronous = NORMAL;",
    "CREATE TABLE users (name TEXT NOT NULL, value INTEGER NOT NULL) STRICT;",
];
const INSERT_USER_SQL: &str = "INSERT INTO users (name, value) VALUES (:name, :value);";
const GET_USERS_SQL: &str = "SELECT name, value FROM users WHERE name = :name;";

fn try_remove_file(path: impl AsRef<Path>) -> std::io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

struct User {
    name: String,
    value: u32,
}

#[async_trait]
trait DatabaseImpl: Clone + Send + Sized + 'static {
    /// Create a new DatabaseImpl
    async fn new() -> anyhow::Result<Self>;

    /// Setup.
    async fn setup(&self) -> anyhow::Result<()>;

    /// Run a write query.
    async fn write(&self, user: User) -> anyhow::Result<()>;

    /// Run a read query.
    async fn read(&self, name: String) -> anyhow::Result<Vec<User>>;

    // Shutdown.
    async fn shutdown(self) -> anyhow::Result<()>;
}

fn spawn_operation<D>(
    rng: &mut StdRng,
    operations_split: f32,
    database_impl: &D,
    set: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<()>
where
    D: DatabaseImpl,
{
    let target_name = NAMES.choose(rng).context("Missing name")?.to_string();
    let is_write = rng.random_range(0.0..1.0) > operations_split;
    let database_impl = database_impl.clone();
    if is_write {
        let user = User {
            name: target_name.clone(),
            value: rng.random_range(..u32::MAX),
        };

        set.spawn(async move {
            database_impl.write(user).await?;
            anyhow::Ok(())
        });
    } else {
        set.spawn(async move {
            let users = database_impl.read(target_name.clone()).await?;
            for user in users {
                assert!(user.name == target_name);
                assert!(user.value != u32::MAX);
            }
            anyhow::Ok(())
        });
    }
    Ok(())
}

async fn run_benchmark<D>(operations_split: f32, rng_seed: u64) -> anyhow::Result<Duration>
where
    D: DatabaseImpl,
{
    let concurrency_limit = 32;

    try_remove_file(DATABASE_PATH)?;
    try_remove_file(format!("{DATABASE_PATH}-shm"))?;
    try_remove_file(format!("{DATABASE_PATH}-wal"))?;

    let database_impl = D::new().await?;

    // TODO: Pregenerate operations.
    let mut rng = StdRng::seed_from_u64(rng_seed);

    database_impl.setup().await?;

    for _ in 0..100 {
        let target_name = NAMES.choose(&mut rng).context("Missing name")?.to_string();
        let user = User {
            name: target_name.clone(),
            value: rng.random_range(..u32::MAX),
        };
        database_impl.write(user).await?;
    }

    let mut set = JoinSet::new();
    let start = Instant::now();
    let mut operation = 0;

    for _ in 0..concurrency_limit {
        spawn_operation(&mut rng, operations_split, &database_impl, &mut set)?;
        operation += 1;
    }

    while let Some(result) = set.join_next().await {
        result??;

        if operation < OPERATIONS {
            spawn_operation(&mut rng, operations_split, &database_impl, &mut set)?;
            operation += 1;
        }
    }

    database_impl.shutdown().await?;

    let end = Instant::now();

    Ok(end - start)
}

async fn async_main() -> anyhow::Result<()> {
    let rng_seed = 12345;
    let operations_split = 0.8;

    let async_connection_time =
        run_benchmark::<nd_async_rusqlite::AsyncConnection>(operations_split, rng_seed).await?;
    let wal_pool_time =
        run_benchmark::<nd_async_rusqlite::WalPool>(operations_split, rng_seed).await?;
    let rusqlite_mutex_time =
        run_benchmark::<Arc<Mutex<rusqlite::Connection>>>(operations_split, rng_seed).await?;
    let tokio_rusqlite_time =
        run_benchmark::<tokio_rusqlite::Connection>(operations_split, rng_seed).await?;

    println!("nd_async_rusqlite::AsyncConnection: {async_connection_time:?}");
    println!("nd_async_rusqlite::WalPool: {wal_pool_time:?}");
    println!("Arc<Mutex<rusqlite::Connection>>: {rusqlite_mutex_time:?}");
    println!("tokio_rusqlite::Connection: {tokio_rusqlite_time:?}");

    Ok(())
}

fn main() -> anyhow::Result<()> {
    LazyLock::force(&NAMES);

    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    tokio_rt.block_on(async_main())?;

    Ok(())
}
