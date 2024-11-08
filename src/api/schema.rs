use async_graphql::{Context, Object, Schema, SimpleObject};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;

const DEFAULT_LIMIT: i32 = 10;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn transactions(
        &self, ctx: &Context<'_>, limit: Option<i32>, offset: Option<i32>, hash: Option<String>,
    ) -> Vec<Transaction> {
        let pool = ctx.data::<PgPool>().unwrap();

        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        let offset = offset.unwrap_or(0);

        let query = if let Some(ref hash) = hash {
            sqlx::query_as::<_, Transaction>(
                "SELECT id, body, type, hash FROM transactions WHERE hash = $1 LIMIT $2 OFFSET $3",
            )
            .bind(hash)
            .bind(limit)
            .bind(offset)
        } else {
            sqlx::query_as::<_, Transaction>(
                "SELECT id, body, type, hash FROM transactions LIMIT $1 OFFSET $2",
            )
            .bind(limit)
            .bind(offset)
        };

        query.fetch_all(pool).await.unwrap()
    }

    async fn jobs(&self, ctx: &Context<'_>, limit: Option<i32>, offset: Option<i32>) -> Vec<Job> {
        let pool = ctx.data::<PgPool>().unwrap();

        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        let offset = offset.unwrap_or(0);

        sqlx::query_as::<_, Job>(
            "SELECT id, transaction_hashes, seq_number FROM jobs LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
        .unwrap()
    }
}

// Define the Transaction struct to match the `transactions` table structure
#[derive(SimpleObject, sqlx::FromRow, Serialize, Deserialize)]
pub struct Transaction {
    pub id: i32,
    pub body: Value,
    #[sqlx(rename = "type")]
    pub type_: String,
    pub hash: String,
}

#[derive(SimpleObject, sqlx::FromRow, Serialize, Deserialize)]
pub struct Job {
    pub id: i32,
    pub transaction_hashes: Vec<String>,
    pub seq_number: i32,
}

pub type MySchema =
    Schema<QueryRoot, async_graphql::EmptyMutation, async_graphql::EmptySubscription>;

pub fn build_schema(pool: PgPool) -> MySchema {
    Schema::build(
        QueryRoot,
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
    )
    .data(pool)
    .finish()
}
