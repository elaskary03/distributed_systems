use axum::{
    extract::{Multipart, Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use image::{imageops::FilterType, DynamicImage, ImageOutputFormat, GenericImageView};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::SystemTime};
use tokio::fs;
use tower_http::cors::{Any, CorsLayer};

#[derive(Parser, Debug)]
#[command(author, version, about = "Client P2P stub server")]
struct Args {
    /// Username owning this P2P server
    #[arg(long, default_value = "user")]
    user: String,

    /// Port to listen on for P2P HTTP
    #[arg(long, default_value_t = 10000)]
    port: u16,
}

#[derive(Clone)]
struct AppState {
    owner: String,
    root: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct Metadata {
    owner: String,
    permissions: HashMap<String, i64>,
    last_update_ns: u128,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
enum PendingUpdate {
    View { requester: String, delta: i64 },
    Perm { target_user: String, new_quota: i64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let root = PathBuf::from("user_images").join(&args.user);
    fs::create_dir_all(&root).await?;
    fs::create_dir_all(root.join("pending_updates")).await?;

    let state = AppState {
        owner: args.user.clone(),
        root,
    };

    let app = Router::new()
        .route("/upload-image", post(upload_image))
        .route("/list-images", get(list_images))
        .route("/preview/:image_id", get(preview_image))
        .route("/full/:image_id", get(full_image))
        .route("/update-permissions/:image_id", post(update_permissions))
        .route("/view-update/:image_id", post(view_update))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("P2P stub listening on {} for owner {}", addr, args.user);
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app)
        .await?;
    Ok(())
}

async fn upload_image(
    State(st): State<AppState>,
    mut mp: Multipart,
) -> impl axum::response::IntoResponse {
    let mut image_id = None;
    let mut owner = None;
    let mut permissions_raw: Option<String> = None;
    let mut image_bytes: Option<Vec<u8>> = None;

    while let Ok(Some(field)) = mp.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "image_id" => image_id = field.text().await.ok(),
            "owner" => owner = field.text().await.ok(),
            "permissions" => permissions_raw = field.text().await.ok(),
            "file" => {
                if let Ok(bytes) = field.bytes().await {
                    image_bytes = Some(bytes.to_vec());
                }
            }
            _ => {}
        }
    }

    let image_id = match image_id {
        Some(v) if !v.is_empty() => v,
        _ => return (StatusCode::BAD_REQUEST, "missing image_id").into_response(),
    };
    let owner = match owner {
        Some(v) if !v.is_empty() => v,
        _ => st.owner.clone(),
    };
    let permissions: HashMap<String, i64> = permissions_raw
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let img = match image_bytes {
        Some(b) => b,
        None => return (StatusCode::BAD_REQUEST, "missing file").into_response(),
    };

    let folder = st.root.parent().unwrap_or(&st.root).join(&owner);
    if let Err(e) = fs::create_dir_all(&folder).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("create dir: {e}"),
        )
            .into_response();
    }
    let _ = fs::create_dir_all(folder.join("pending_updates")).await;

    if let Err(e) = save_image(&folder, &image_id, &img).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("save image: {e}"),
        )
            .into_response();
    }

    let meta = Metadata {
        owner: owner.clone(),
        permissions,
        last_update_ns: now_nanos(),
    };
    if let Err(e) = save_metadata(&folder, &image_id, &meta).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("save meta: {e}"),
        )
            .into_response();
    }

    let _ = replay_pending(&folder, &image_id).await;

    Json(json!({"status":"ok"})).into_response()
}

async fn list_images(State(st): State<AppState>) -> impl axum::response::IntoResponse {
    let mut images = Vec::new();
    if let Ok(mut rd) = fs::read_dir(&st.root).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                if let Some(id) = entry
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
                {
                    if let Ok(meta) = load_metadata(&st.root, &id).await {
                        images.push(json!({
                            "id": id,
                            "owner": meta.owner,
                            "permissions": meta.permissions,
                            "last_update_ns": meta.last_update_ns
                        }));
                    }
                }
            }
        }
    }
    Json(json!({"status":"ok","images": images}))
}

async fn preview_image(
    State(st): State<AppState>,
    AxumPath(image_id): AxumPath<String>,
) -> impl axum::response::IntoResponse {
    let img_bytes = match load_image(&st.root, &image_id).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let img = match image::load_from_memory(&img_bytes) {
        Ok(i) => i,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let preview = resize_to_width(img, 200);
    let mut buf = Vec::new();
    if preview
        .write_to(&mut std::io::Cursor::new(&mut buf), ImageOutputFormat::Png)
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    (StatusCode::OK, [("content-type", "image/png")], buf).into_response()
}

async fn full_image(
    State(st): State<AppState>,
    AxumPath(image_id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    let requester = params.get("requester");
    if let Ok(mut meta) = load_metadata(&st.root, &image_id).await {
        if let Some(r) = requester {
            // Owner always allowed without quota changes
            let mut should_decrement = true;
            if *r == meta.owner {
                should_decrement = false;
            } else if meta.permissions.is_empty() {
                // No permissions configured yet: allow by default (scaffolding mode)
                should_decrement = false;
            } else {
                let quota = meta.permissions.get(r).copied().unwrap_or(0);
                if quota <= 0 {
                    return denied_image();
                }
                meta.permissions.insert(r.clone(), quota - 1);
            }
            if should_decrement {
                meta.last_update_ns = now_nanos();
                let _ = save_metadata(&st.root, &image_id, &meta).await;
            }
        }
        let _ = replay_pending(&st.root, &image_id).await;
    } else {
        let _ = enqueue_pending(&st.root, &image_id, PendingUpdate::View {
            requester: requester.cloned().unwrap_or_default(),
            delta: -1,
        }).await;
    }

    match load_image(&st.root, &image_id).await {
        Ok(bytes) => (StatusCode::OK, [("content-type", "image/png")], bytes).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn view_update(
    State(st): State<AppState>,
    AxumPath(image_id): AxumPath<String>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let requester = body.get("requester").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let delta = body.get("delta").and_then(|v| v.as_i64()).unwrap_or(0);

    if let Ok(mut meta) = load_metadata(&st.root, &image_id).await {
        let entry = meta.permissions.entry(requester.clone()).or_insert(0);
        *entry = (*entry + delta).max(0);
        meta.last_update_ns = now_nanos();
        let _ = save_metadata(&st.root, &image_id, &meta).await;
        let _ = replay_pending(&st.root, &image_id).await;
        Json(json!({"status":"ok","image_id": image_id, "permissions": meta.permissions})).into_response()
    } else {
        let _ = enqueue_pending(&st.root, &image_id, PendingUpdate::View { requester, delta }).await;
        Json(json!({"status":"queued","image_id": image_id})).into_response()
    }
}

async fn update_permissions(
    State(st): State<AppState>,
    AxumPath(image_id): AxumPath<String>,
    Json(body): Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let target_user = body
        .get("target_user")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let new_quota = body
        .get("new_quota")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if target_user.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing target_user").into_response();
    }

    if let Ok(mut meta) = load_metadata(&st.root, &image_id).await {
        meta.permissions.insert(target_user.clone(), new_quota);
        meta.last_update_ns = now_nanos();
        let _ = save_metadata(&st.root, &image_id, &meta).await;
        let _ = replay_pending(&st.root, &image_id).await;
        Json(json!({"status":"ok","image_id": image_id, "permissions": meta.permissions})).into_response()
    } else {
        let _ = enqueue_pending(
            &st.root,
            &image_id,
            PendingUpdate::Perm {
                target_user,
                new_quota,
            },
        )
        .await;
        Json(json!({"status":"queued","image_id": image_id})).into_response()
    }
}

// helpers
async fn load_metadata(root: &PathBuf, id: &str) -> Result<Metadata, anyhow::Error> {
    let path = root.join(format!("{}.json", id));
    let data = fs::read(path).await?;
    let meta: Metadata = serde_json::from_slice(&data)?;
    Ok(meta)
}

async fn save_metadata(root: &PathBuf, id: &str, meta: &Metadata) -> Result<(), anyhow::Error> {
    let path = root.join(format!("{}.json", id));
    let data = serde_json::to_vec_pretty(meta)?;
    fs::write(path, data).await?;
    Ok(())
}

async fn load_image(root: &PathBuf, id: &str) -> Result<Vec<u8>, anyhow::Error> {
    let path = root.join(format!("{}.png", id));
    Ok(fs::read(path).await?)
}

async fn save_image(root: &PathBuf, id: &str, bytes: &[u8]) -> Result<(), anyhow::Error> {
    let path = root.join(format!("{}.png", id));
    fs::write(path, bytes).await?;
    Ok(())
}

async fn enqueue_pending(root: &PathBuf, id: &str, upd: PendingUpdate) -> Result<(), anyhow::Error> {
    let dir = root.join("pending_updates");
    fs::create_dir_all(&dir).await.ok();
    let path = dir.join(format!("{}.json", id));
    let mut list: Vec<PendingUpdate> = if let Ok(data) = fs::read(&path).await {
        serde_json::from_slice(&data).unwrap_or_default()
    } else {
        Vec::new()
    };
    list.push(upd);
    let data = serde_json::to_vec_pretty(&list)?;
    fs::write(path, data).await?;
    Ok(())
}

async fn replay_pending(root: &PathBuf, id: &str) -> Result<(), anyhow::Error> {
    let path = root.join("pending_updates").join(format!("{}.json", id));
    let data = match fs::read(&path).await {
        Ok(d) => d,
        Err(_) => return Ok(()),
    };
    let updates: Vec<PendingUpdate> = serde_json::from_slice(&data).unwrap_or_default();
    if updates.is_empty() {
        let _ = fs::remove_file(&path).await;
        return Ok(());
    }

    let mut meta = load_metadata(root, id).await?;
    for upd in updates {
        match upd {
            PendingUpdate::View { requester, delta } => {
                let entry = meta.permissions.entry(requester).or_insert(0);
                *entry = (*entry + delta).max(0);
            }
            PendingUpdate::Perm { target_user, new_quota } => {
                meta.permissions.insert(target_user, new_quota);
            }
        }
    }
    meta.last_update_ns = now_nanos();
    save_metadata(root, id, &meta).await?;
    let _ = fs::remove_file(&path).await;
    Ok(())
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

fn denied_image() -> axum::response::Response {
    let mut buf = Vec::new();
    let mut img = DynamicImage::new_rgba8(300, 200).to_rgba8();
    for p in img.pixels_mut() {
        *p = image::Rgba([255, 0, 0, 255]);
    }
    let dynimg = DynamicImage::ImageRgba8(img);
    if dynimg
        .write_to(&mut std::io::Cursor::new(&mut buf), ImageOutputFormat::Png)
        .is_ok()
    {
        return (StatusCode::FORBIDDEN, [("content-type", "image/png")], buf).into_response();
    }
    StatusCode::FORBIDDEN.into_response()
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
