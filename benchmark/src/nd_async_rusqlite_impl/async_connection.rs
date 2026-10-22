use crate::DATABASE_PATH;
use crate::DatabaseImpl;
use crate::GET_USERS_SQL;
use crate::INSERT_USER_SQL;
use crate::SET_WAL_MODE_SQL;
use crate::SETUP_SQL;
use crate::User;
use async_trait::async_trait;
use nd_async_rusqlite::rusqlite::named_params;

#[async_trait]
impl DatabaseImpl for nd_async_rusqlite::AsyncConnection {
    async fn new() -> anyhow::Result<Self> {
        let connection = nd_async_rusqlite::AsyncConnection::builder()
            .open(DATABASE_PATH)
            .await?;

        Ok(connection)
    }

    async fn setup(&self) -> anyhow::Result<()> {
        self.access(move |database| {
            database.query_one(SET_WAL_MODE_SQL, [], |row| {
                let value: String = row.get(0)?;
                Ok(value)
            })?;
            for sql in SETUP_SQL {
                database.execute(sql, [])?;
            }

            anyhow::Ok(())
        })
        .await??;

        Ok(())
    }

    async fn write(&self, user: User) -> anyhow::Result<()> {
        self.access(move |database| {
            let transaction = database.transaction()?;
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
        let users = self
            .access(move |database| {
                let users = database
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
        self.access(move |database| {
            database.execute("VACUUM;", [])?;

            anyhow::Ok(())
        })
        .await??;
        self.close().await?;

        Ok(())
    }
}
