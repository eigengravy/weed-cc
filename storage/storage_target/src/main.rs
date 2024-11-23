use axum::{
    routing::post,
    extract::{Json, State},
    response::IntoResponse,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CsvRow {
    id: usize,
    data: String,
    timestamp: u64,
}

#[derive(Clone)]
struct AppState {
    file_path: String,
    rows: Arc<Mutex<HashMap<usize, CsvRow>>>, // In-memory store for deduplication and updates
}

async fn store_row(Json(row): Json<CsvRow>, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut rows = state.rows.lock().await;

    // Check if the row already exists
    if rows.contains_key(&row.id) {
        println!("Updating existing row: {:?}", row.id);
        rows.insert(row.id, row.clone()); // Update in-memory store
    } else {
        println!("Storing new row: {:?}", row.id);
        rows.insert(row.id, row.clone()); // Add to in-memory store
    }

    // Write to file (append or replace by overwriting the entire file for simplicity)
    let file_path = &state.file_path;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true) // Rewrite file completely to reflect the latest state
        .open(file_path)
        .expect("Unable to open file");

    let mut writer = BufWriter::new(file);
    for row in rows.values() {
        writeln!(writer, "{},{},{}", row.id, row.data, row.timestamp)
            .expect("Failed to write row to file");
    }

    "Row stored or updated successfully"
}

#[tokio::main]
async fn main() {
    let file_path = std::env::var("FILE_PATH").expect("FILE_PATH not set");
    let state = Arc::new(AppState {
        file_path,
        rows: Arc::new(Mutex::new(HashMap::new())),
    });

    let app = Router::new()
        .route("/store", post(store_row))
        .route("/fetch", post(fetch_rows))
        .route("/sync", post(sync_rows))
        .with_state(state);

    axum::Server::bind(&"0.0.0.0:8080".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn fetch_rows(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows = state.rows.lock().await;
    let result: Vec<CsvRow> = rows.values().cloned().collect();
    Json(result)
}

async fn sync_rows(Json(rows): Json<Vec<CsvRow>>, State(state): State<Arc<AppState>>) -> &'static str {
    let mut store = state.rows.lock().await;
    for row in rows {
        store.entry(row.id).or_insert(row);
    }
    "Sync completed"
}
