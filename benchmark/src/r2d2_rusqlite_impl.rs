use crate::DATABASE_PATH;
use crate::DatabaseImpl;
use crate::GET_USERS_SQL;
use crate::INSERT_USER_SQL;
use crate::SET_WAL_MODE_SQL;
use crate::SETUP_SYNCHRONOUS_SQL;
use crate::SETUP_TABLE_SQL;
use crate::User;
use async_trait::async_trait;
use nd_async_rusqlite::rusqlite;
use nd_async_rusqlite::rusqlite::named_params;

#[derive(Debug)]
struct SqliteCustomizer;

impl r2d2::CustomizeConnection<rusqlite::Connection, rusqlite::Error> for SqliteCustomizer {
    fn on_acquire(&self, connection: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        connection.query_one(SET_WAL_MODE_SQL, [], |row| {
            let value: String = row.get(0)?;
            Ok(value)
        })?;
        connection.execute(SETUP_SYNCHRONOUS_SQL, [])?;

        Ok(())
    }
}

#[async_trait]
impl DatabaseImpl for r2d2::Pool<r2d2_sqlite::SqliteConnectionManager> {
    async fn new() -> anyhow::Result<Self> {
        let manager = r2d2_sqlite::SqliteConnectionManager::file(DATABASE_PATH);
        let pool = r2d2::Pool::builder()
            .connection_customizer(Box::new(SqliteCustomizer))
            .build(manager)?;

        Ok(pool)
    }

    async fn setup(&self) -> anyhow::Result<()> {
        let self_clone = self.clone();

        tokio::task::spawn_blocking(move || {
            let connection = self_clone.get()?;

            connection.execute(SETUP_TABLE_SQL, [])?;

            anyhow::Ok(())
        })
        .await??;

        Ok(())
    }

    async fn write(&self, user: User) -> anyhow::Result<()> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = self_clone.get()?;

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
            let connection = self_clone.get()?;

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

    async fn shutdown(self) -> anyhow::Result<()> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            let connection = self_clone.get()?;

            connection.execute("VACUUM;", [])?;

            anyhow::Ok(())
        })
        .await??;

        Ok(())
    }
}
