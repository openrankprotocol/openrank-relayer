use async_graphql::{Context, Object, Schema, SimpleObject};
use async_graphql::{EmptyMutation, EmptySubscription};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sqlx::{query_as, PgPool};

const DEFAULT_LIMIT: i32 = 10;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn transactions(
        &self, ctx: &Context<'_>, limit: Option<i32>, offset: Option<i32>, hash: Option<String>,
        job_seq_number: Option<i32>,
    ) -> Vec<Transaction> {
        let pool = ctx.data::<PgPool>().unwrap();

        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        let offset = offset.unwrap_or(0);

        let mut sql = String::from("SELECT id, body, type, hash, job_seq_number, \"to\", \"from\" FROM transactions WHERE 1=1");
        let mut index = 1;

        if hash.is_some() {
            sql.push_str(&format!(" AND hash = ${}", index));
            index += 1;
        }
        if job_seq_number.is_some() {
            sql.push_str(&format!(" AND job_seq_number = ${}", index));
            index += 1;
        }

        sql.push_str(&format!(" LIMIT ${} OFFSET ${}", index, index + 1));

        let mut query = query_as::<_, Transaction>(&sql);

        if let Some(h) = hash {
            query = query.bind(h);
        }
        if let Some(jsn) = job_seq_number {
            query = query.bind(jsn);
        }
        query = query.bind(limit).bind(offset);

        query.fetch_all(pool).await.unwrap()
    }

    async fn jobs(&self, ctx: &Context<'_>, limit: Option<i32>, offset: Option<i32>) -> Vec<Job> {
        let pool = ctx.data::<PgPool>().unwrap();

        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        let offset = offset.unwrap_or(0);

        sqlx::query_as::<_, Job>(
            "SELECT id, transaction_hashes, seq_number, timestamp FROM jobs LIMIT $1 OFFSET $2",
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
    pub job_seq_number: i32,
    pub to: String,
    pub from: String,
}

#[derive(SimpleObject, sqlx::FromRow, Serialize, Deserialize)]
pub struct Job {
    pub id: i32,
    pub transaction_hashes: Vec<String>,
    pub seq_number: i32,
    pub timestamp: i32,
}

pub type MySchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn build_schema(pool: PgPool) -> MySchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription).data(pool).finish()
}
