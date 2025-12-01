use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use image::{imageops::FilterType, DynamicImage, GenericImageView};
use serde::{Deserialize, Serialize};
use tokio::{fs, net::TcpListener};

#[derive(Parser, Debug)]
#[command(author, version, about = "P2P user server (Phase 2 scaffold)")]
struct Args {
    /// Username owning this P2P server
    #[arg(long)]
    user: String,

    /// Port to listen on
    #[arg(long, default_value_t = 10000)]
    port: u16,
}

#[derive(Clone)]
struct AppState {
    user: String,
    dir: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Metadata {
    owner: String,
    permissions: HashMap<String, i64>,
    last_update_ns: u128,
}

#[derive(Deserialize)]
struct UpdateViewReq {
    requester: String,
    delta: i64,
}

#[derive(Deserialize)]
struct UpdatePermissionsReq {
    target_user: String,
    new_quota: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let root = PathBuf::from("user_images").join(&args.user);
    fs::create_dir_all(&root).await?;

    let state = AppState {
        user: args.user.clone(),
        dir: root,
    };

    let app = Router::new()
        .route("/list-images", get(list_images))
        .route("/preview/:id", get(preview_image))
        .route("/full/:id", get(full_image))
        .route("/view-update/:id", post(view_update))
        .route("/update-permissions/:id", post(update_permissions))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!(
        "P2P server for {} running at http://{}",
        args.user, addr
    );
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn list_images(
    State(st): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let requester = params.get("requester").cloned();
    let mut out = Vec::new();
    let mut dir = fs::read_dir(&st.dir).await.unwrap_or_else(|_| panic!("dir missing: {:?}", st.dir));
    while let Ok(Some(entry)) = dir.next_entry().await {
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Some(id) = entry
            .path()
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
        {
            if let Ok(meta) = load_metadata(&st, &id).await {
                let remaining = requester
                    .as_ref()
                    .and_then(|r| meta.permissions.get(r))
                    .copied()
                    .unwrap_or(0);
                out.push(serde_json::json!({
                    "id": id,
                    "owner": meta.owner,
                    "permissions": meta.permissions,
                    "remaining_views_for_requester": remaining
                }));
            }
        }
    }
    Json(out)
}

async fn preview_image(
    State(st): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    match load_image(&st, &id).await {
        Ok(img_bytes) => match image::load_from_memory(&img_bytes) {
            Ok(img) => {
                let resized = resize_to_width(img, 200);
                let mut buf = Vec::new();
                if resized.write_to(&mut std::io::Cursor::new(&mut buf), image::ImageOutputFormat::Png).is_ok() {
                    return (StatusCode::OK, [("content-type", "image/png")], buf).into_response();
                }
            }
            Err(_) => {}
        },
        Err(_) => {}
    }
    StatusCode::NOT_FOUND.into_response()
}

async fn full_image(
    State(st): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let requester = match params.get("requester") {
        Some(r) => r.clone(),
        None => return (StatusCode::BAD_REQUEST, "missing requester").into_response(),
    };

    let mut meta = match load_metadata(&st, &id).await {
        Ok(m) => m,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let quota = meta.permissions.get(&requester).copied().unwrap_or(0);
    if quota <= 0 {
        return denied_image();
    }

    // decrement and persist
    meta.permissions.insert(requester.clone(), quota - 1);
    meta.last_update_ns = now_nanos();
    let _ = save_metadata(&st, &id, &meta).await;

    match load_image(&st, &id).await {
        Ok(bytes) => (StatusCode::OK, [("content-type", "image/png")], bytes).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn view_update(
    State(st): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<UpdateViewReq>,
) -> impl IntoResponse {
    let mut meta = load_metadata(&st, &id).await.unwrap_or(Metadata {
        owner: st.user.clone(),
        permissions: HashMap::new(),
        last_update_ns: now_nanos(),
    });
    let entry = meta.permissions.entry(body.requester).or_insert(0);
    *entry = (*entry + body.delta).max(0);
    meta.last_update_ns = now_nanos();
    let _ = save_metadata(&st, &id, &meta).await;
    Json(serde_json::json!({"status":"ok","permissions": meta.permissions}))
}

async fn update_permissions(
    State(st): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<UpdatePermissionsReq>,
) -> impl IntoResponse {
    let mut meta = load_metadata(&st, &id).await.unwrap_or(Metadata {
        owner: st.user.clone(),
        permissions: HashMap::new(),
        last_update_ns: now_nanos(),
    });
    meta.permissions.insert(body.target_user, body.new_quota);
    meta.last_update_ns = now_nanos();
    let _ = save_metadata(&st, &id, &meta).await;
    Json(serde_json::json!({"status":"ok","permissions": meta.permissions}))
}

async fn load_metadata(st: &AppState, id: &str) -> Result<Metadata, anyhow::Error> {
    let path = st.dir.join(format!("{}.json", id));
    let data = fs::read(&path).await?;
    let meta: Metadata = serde_json::from_slice(&data)?;
    Ok(meta)
}

async fn save_metadata(st: &AppState, id: &str, meta: &Metadata) -> Result<(), anyhow::Error> {
    let path = st.dir.join(format!("{}.json", id));
    let data = serde_json::to_vec_pretty(meta)?;
    fs::write(path, data).await?;
    Ok(())
}

async fn load_image(st: &AppState, id: &str) -> Result<Vec<u8>, anyhow::Error> {
    let path = st.dir.join(format!("{}.png", id));
    let data = fs::read(path).await?;
    Ok(data)
}

fn resize_to_width(img: DynamicImage, width: u32) -> DynamicImage {
    let (w, h) = img.dimensions();
    if w <= width {
        img
    } else {
        let new_h = ((h as f32) * (width as f32 / w as f32)).round().max(1.0) as u32;
        DynamicImage::ImageRgba8(image::imageops::resize(
            &img.to_rgba8(),
            width,
            new_h,
            FilterType::Lanczos3,
        ))
    }
}

fn denied_image() -> Response {
    let img = DynamicImage::new_rgba8(300, 200);
    let mut buf = Vec::new();
    let mut red = img.to_rgba8();
    for p in red.pixels_mut() {
        *p = image::Rgba([255, 0, 0, 255]);
    }
    let dynimg = DynamicImage::ImageRgba8(red);
    if dynimg
        .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageOutputFormat::Png)
        .is_ok()
    {
        return (StatusCode::FORBIDDEN, [("content-type", "image/png")], buf).into_response();
    }
    StatusCode::FORBIDDEN.into_response()
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
