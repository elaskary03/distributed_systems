use axum::{routing::{get, post}, Router, Json};
use clap::Parser;
use serde_json::json;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(author, version, about = "Client P2P stub server")]
struct Args {
    /// Port to listen on for P2P HTTP
    #[arg(long, default_value_t = 10000)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let app = Router::new()
        .route("/list-images", get(list_images))
        .route("/preview/:image_id", get(preview_image))
        .route("/full/:image_id", get(full_image))
        .route("/view-update/:image_id", post(view_update));

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("P2P stub listening on {}", addr);
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

async fn list_images() -> Json<serde_json::Value> {
    Json(json!({"status":"ok","images":[]}))
}

async fn preview_image(axum::extract::Path(image_id): axum::extract::Path<String>) -> Json<serde_json::Value> {
    Json(json!({"status":"ok","image_id": image_id, "preview":"stub"}))
}

async fn full_image(axum::extract::Path(image_id): axum::extract::Path<String>) -> Json<serde_json::Value> {
    Json(json!({"status":"ok","image_id": image_id, "data":"stub"}))
}

async fn view_update(axum::extract::Path(image_id): axum::extract::Path<String>) -> Json<serde_json::Value> {
    Json(json!({"status":"ok","image_id": image_id, "updated": true}))
}
