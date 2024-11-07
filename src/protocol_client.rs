use reqwest::Client;
use serde_json::json;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct RpcClient {
    client: Client,
    url: String,
    request_id: AtomicU64,
}

impl RpcClient {
    pub fn new(url: &str) -> Self {
        RpcClient { client: Client::new(), url: url.to_string(), request_id: AtomicU64::new(1) }
    }

    fn get_next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn sequencer_get_compute_result(
        &self, id: u64,
    ) -> Result<serde_json::Value, Box<dyn Error>> {
        let request_id = self.get_next_id();
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "sequencer_get_compute_result",
            "params": [id],
            "id": request_id,
        });

        let response = self.client.post(&self.url).json(&payload).send().await?.json().await?;

        Ok(response)
    }

    pub async fn sequencer_get_results(
        &self, request_tx_hash: &str, start: u64, size: u64,
    ) -> Result<serde_json::Value, Box<dyn Error>> {
        let request_id = self.get_next_id();
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "sequencer_get_results",
            "params": [{
                "request_tx_hash": request_tx_hash,
                "start": start,
                "size": size,
            }],
            "id": request_id,
        });

        let response = self.client.post(&self.url).json(&payload).send().await?.json().await?;

        Ok(response)
    }

    pub async fn sequencer_get_tx(
        &self, tx_type: &str, tx_hash: &str,
    ) -> Result<serde_json::Value, Box<dyn Error>> {
        let request_id = self.get_next_id();
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "sequencer_get_tx",
            "params": [tx_type, tx_hash],
            "id": request_id,
        });

        let response = self.client.post(&self.url).json(&payload).send().await?.json().await?;

        Ok(response)
    }
}

/*
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = RpcClient::new("https://or-dev-prod.k3l.io");

    // Call sequencer_get_compute_result
    let compute_result = client.sequencer_get_compute_result(0).await?;
    println!("Compute Result: {:?}", compute_result);

    // Call sequencer_get_results
    let results = client.sequencer_get_results("35b8f06856b66b27b746c33fb8d779e23bc62715baf23d88c5ac31527d43d4d5", 0, 10).await?;
    println!("Results: {:?}", results);

    // Call sequencer_get_tx
    let tx = client.sequencer_get_tx("compute_request", "35b8f06856b66b27b746c33fb8d779e23bc62715baf23d88c5ac31527d43d4d5").await?;
    println!("Transaction: {:?}", tx);

    Ok(())
}
    */
