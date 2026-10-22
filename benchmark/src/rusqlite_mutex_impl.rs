use crate::DATABASE_PATH;
use crate::DatabaseImpl;
use crate::GET_USERS_SQL;
use crate::INSERT_USER_SQL;
use crate::SET_WAL_MODE_SQL;
use crate::SETUP_SQL;
use crate::User;
use async_trait::async_trait;
use nd_async_rusqlite::rusqlite;
use nd_async_rusqlite::rusqlite::named_params;
use std::sync::Arc;
use std::sync::Mutex;

#[async_trait]
impl DatabaseImpl for Arc<Mutex<rusqlite::Connection>> {
    async fn new() -> anyhow::Result<Self> {
        Ok(Arc::new(Mutex::new(rusqlite::Connection::open(
            DATABASE_PATH,
        )?)))
    }

    async fn setup(&self) -> anyhow::Result<()> {
        let self_clone = self.clone();

        tokio::task::spawn_blocking(move || {
            let connection = self_clone.lock().unwrap_or_else(|error| error.into_inner());

            connection.query_one(SET_WAL_MODE_SQL, [], |row| {
                let value: String = row.get(0)?;
                Ok(value)
            })?;

            for sql in SETUP_SQL {
                connection.execute(sql, [])?;
            }

            anyhow::Ok(())
        })
        .await??;

        Ok(())
    }

    async fn write(&self, user: User) -> anyhow::Result<()> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = self_clone.lock().unwrap_or_else(|error| error.into_inner());

            let transaction = connection.transaction()?;
            transaction
                .prepare_cached(INSERT_USER_SQL)?
                .execute(named_params! {
                    ":name": user.name,
                    ":value": user.value,
                })?;
            transaction.commit()?;

            anyhow::Ok(())
        })
        .await??;

        Ok(())
    }

    async fn read(&self, _name: String) -> anyhow::Result<Vec<User>> {
        let self_clone = self.clone();

        let users = tokio::task::spawn_blocking(move || {
            let connection = self_clone.lock().unwrap_or_else(|error| error.into_inner());

            let users = connection
                .prepare_cached(GET_USERS_SQL)?
                .query_map(named_params! {}, |row| {
                    let name = row.get("name")?;
                    let value = row.get("value")?;

                    Ok(User { name, value })
                })?
                .collect::<Result<_, _>>()?;

            anyhow::Ok(users)
        })
        .await??;

        Ok(users)
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        let self_clone = self.clone();

        tokio::task::spawn_blocking(move || {
            let connection = self_clone.lock().unwrap_or_else(|error| error.into_inner());

            connection.execute("VACUUM;", [])?;

            anyhow::Ok(())
        })
        .await??;

        Ok(())
    }
}
