use crate::protocol_client::RpcClient;
use crate::types::TxWithHash;
use log::{error, info};
use openrank_common::tx::{self, compute, consts};
use std::collections::HashMap;
use tokio::time::Duration;

mod postgres;
mod protocol_client;
mod types;

const INTERVAL_SECONDS: u64 = 10;

pub struct SQLRelayer {
    target_db: postgres::SQLDatabase,
    protocol_client: RpcClient,
}

impl SQLRelayer {
    pub async fn init(is_reindex: bool) -> Self {
        let target_db = postgres::SQLDatabase::connect().await.expect("Connect to Postgres db");

        if is_reindex {
            log::info!("Reindexing: dropping tables.");
            target_db.drop_tables().await.unwrap();
        }

        target_db.init().await.unwrap();

        let protocol_client = RpcClient::new("https://or-dev-prod.k3l.io");

        SQLRelayer { target_db, protocol_client }
    }

    async fn save_last_processed_key(&self, db_path: &str, last_processed_key: usize) {
        self.target_db
            .save_last_processed_key(
                &format!("relayer_last_key_{}", db_path),
                last_processed_key as i32,
            )
            .await
            .expect("Failed to save last processed key");
    }

    async fn index(&mut self) {
        let last_count = self
            .target_db
            .load_last_processed_key("jobs")
            .await
            .expect("Failed to load last processed key")
            .unwrap_or(0);

        let mut current_count = last_count;

        log::info!("Indexing db, last_count: {:?}", last_count);
        loop {
            let compute_result = self
                .protocol_client
                .sequencer_get_compute_result(current_count.try_into().unwrap())
                .await
                .unwrap();

            if let Some(error) = compute_result.get("error") {
                error!("{:?}", compute_result);
                break;
            }
            let result = compute_result.get("result").unwrap();

            let mut hashes = vec![
                result
                    .get("compute_commitment_tx_hash")
                    .and_then(|v| v.as_str())
                    .expect("must be a string")
                    .to_string(),
                result
                    .get("compute_request_tx_hash")
                    .and_then(|v| v.as_str())
                    .expect("must be a string")
                    .to_string(),
            ];

            if let Some(verification_hashes) =
                result.get("compute_verification_tx_hashes").and_then(|v| v.as_array())
            {
                hashes.extend(
                    verification_hashes.iter().filter_map(|v| v.as_str().map(|s| s.to_string())),
                );
            }

            let seq_number = result
                .get("seq_number")
                .and_then(|v| v.as_i64())
                .expect("seq_number should be an integer") as i32;

            self.target_db.insert_job(seq_number, hashes).await.unwrap();

            current_count = current_count + 1;
        }

        if last_count < current_count {
            self.save_last_processed_key("jobs", current_count).await;
        }
    }

    pub async fn start(&mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(INTERVAL_SECONDS));

        loop {
            interval.tick().await;
            info!("Running periodic index check...");
            self.index().await;
        }
    }
}
