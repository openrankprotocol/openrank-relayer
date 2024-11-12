use crate::protocol_client::RpcClient;
use async_recursion::async_recursion;
use log::info;
use std::env;
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

        let url = env::var("PROTOCOL_RPC_URL").expect("PROTOCOL_RPC_URL must be set");
        let protocol_client = RpcClient::new(&url);

        SQLRelayer { target_db, protocol_client }
    }

    async fn save_last_processed_key(&self, db_path: &str, last_processed_key: usize) {
        self.target_db
            .save_last_processed_key(db_path, last_processed_key as i32)
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

            if compute_result.get("error").is_some() {
                // error!("{:?}", compute_result);
                break;
            }
            let result = compute_result.get("result").unwrap();

            let compute_commitment_tx_hash = result
                .get("compute_commitment_tx_hash")
                .and_then(|v| v.as_str())
                .expect("must be a string")
                .to_string();

            let compute_request_tx_hash = result
                .get("compute_request_tx_hash")
                .and_then(|v| v.as_str())
                .expect("must be a string")
                .to_string();

            let mut hashes =
                vec![compute_commitment_tx_hash.clone(), compute_request_tx_hash.clone()];

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

            self.target_db.insert_job(seq_number, hashes.clone()).await.unwrap();

            let mut transactions = vec![
                ("compute_commitment", compute_commitment_tx_hash.clone()),
                ("compute_request", compute_request_tx_hash.clone()),
            ];

            let verification_transactions: Vec<(&str, String)> = result
                .get("compute_verification_tx_hashes")
                .and_then(|v| v.as_array())
                .map(|hashes| {
                    hashes
                        .iter()
                        .filter_map(|hash| {
                            hash.as_str().map(|s| ("compute_verification", s.to_string()))
                        })
                        .collect::<Vec<(&str, String)>>()
                })
                .unwrap_or_else(Vec::new);

            transactions.extend(verification_transactions);

            for (tx_type, hash) in transactions {
                self.process_transaction(current_count.try_into().unwrap(), tx_type, &hash).await;
            }

            current_count += 1;
            if last_count < current_count {
                self.save_last_processed_key("jobs", current_count).await;
            }
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

    #[async_recursion]
    async fn process_transaction(&self, seq_id: i32, tx_type: &str, hash: &str) {
        let res = self
            .protocol_client
            .sequencer_get_tx(tx_type, hash)
            .await
            .expect("Failed to get transaction");

        let body = res.pointer("/result/body").expect("Missing txn body").to_string();
        let to = res
            .pointer("/result/to")
            .and_then(|v| v.as_str())
            .expect("must be a string")
            .to_string();
        let from = res
            .pointer("/result/from")
            .and_then(|v| v.as_str())
            .expect("must be a string")
            .to_string();

        if tx_type == "compute_commitment" {
            let assignment_tx_hash = res
                .pointer("/result/body/ComputeCommitment/assignment_tx_hash")
                .and_then(|v| v.as_str())
                .expect("must be a string")
                .to_string();

            self.process_transaction(seq_id, "compute_assignment", &assignment_tx_hash).await;

            if let Some(scores_tx_hashes) = res
                .pointer("/result/body/ComputeCommitment/scores_tx_hashes")
                .and_then(|v| v.as_array())
            {
                for score_tx_hash in scores_tx_hashes {
                    if let Some(score_tx_hash_str) = score_tx_hash.as_str() {
                        self.process_transaction(seq_id, "compute_scores", score_tx_hash_str).await;
                    }
                }
            }
        }

        let _ = self.target_db.insert_transactions(seq_id, hash, &body, tx_type, &to, &from).await;
    }
}
