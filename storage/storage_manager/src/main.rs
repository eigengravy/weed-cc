use tokio::sync::mpsc;
use std::{collections::HashMap, time::{Duration, SystemTime}};
use serde_json::json;
use reqwest::Client;
use axum::{Router, routing::get, Json, State};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CsvRow {
    id: usize,
    data: String,
    timestamp: u64, // UNIX timestamp for time-based filtering
}

struct StorageManager {
    metadata: HashMap<usize, Vec<String>>, // Row ID -> Target URLs
    targets: Vec<String>,                 // Target URLs
    local_store: HashMap<usize, CsvRow>,  // Locally stored rows for resilience
}

impl StorageManager {
    fn new() -> Self {
        Self {
            metadata: HashMap::new(),
            targets: Vec::new(),
            local_store: HashMap::new(),
        }
    }

    fn add_target(&mut self, target_url: String) {
        self.targets.push(target_url);
    }

    async fn process_row(&mut self, row: CsvRow) {
        let target_count = self.targets.len();
        if target_count < 2 {
            panic!("At least 2 storage targets are required");
        }

        let target1 = row.id % target_count;
        let target2 = (row.id + 1) % target_count;

        let client = Client::new();
        let mut success_count = 0;

        for &target in &[target1, target2] {
            let target_url = &self.targets[target];
            let payload = json!(row);

            match client.post(target_url).json(&payload).send().await {
                Ok(_) => success_count += 1,
                Err(_) => {
                    println!("Target {} failed, storing locally", target);
                    self.local_store.insert(row.id, row.clone());
                }
            }
        }

        if success_count > 0 {
            self.metadata.insert(
                row.id,
                vec![self.targets[target1].clone(), self.targets[target2].clone()],
            );
        }
    }

    async fn fetch_rows(&self, past_seconds: u64) -> Vec<CsvRow> {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let client = Client::new();
        let mut all_rows = HashMap::new();

        for target_url in &self.targets {
            let response = client
                .get(format!("{}/fetch?past_seconds={}", target_url, past_seconds))
                .send()
                .await;

            if let Ok(res) = response {
                if let Ok(rows): Result<Vec<CsvRow>, _> = res.json().await {
                    for row in rows {
                        all_rows.entry(row.id).or_insert(row);
                    }
                }
            } else {
                println!("Target {} is down, skipping.", target_url);
            }
        }

        // Filter rows from the past `x` seconds
        all_rows
            .values()
            .filter(|row| now - row.timestamp <= past_seconds)
            .cloned()
            .collect()
    }

    async fn sync_target(&self, target_url: &str) {
        let client = Client::new();

        for row in self.local_store.values() {
            let payload = json!(row);
            if let Ok(_) = client.post(target_url).json(&payload).send().await {
                println!("Synced row {} to {}", row.id, target_url);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let manager = StorageManager::new();

    // Initialize targets
    manager.add_target("http://storage_target1:8080".to_string());
    manager.add_target("http://storage_target2:8080".to_string());

    let state = axum::extract::State(manager);

    // API routes
    let app = Router::new()
        .route("/store", get(store_handler)) // POST endpoint for writing rows
        .route("/read", get(read_handler))  // GET endpoint for fetching rows
        .with_state(state);

    axum::Server::bind(&"0.0.0.0:8080".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn store_handler(Json(row): Json<CsvRow>, State(mut manager): State<StorageManager>) -> &'static str {
    manager.process_row(row).await;
    "Row stored successfully"
}

async fn read_handler(State(manager): State<StorageManager>) -> Json<Vec<CsvRow>> {
    let rows = manager.fetch_rows(60).await; // Fetch rows from the past 60 seconds
    Json(rows)
}
