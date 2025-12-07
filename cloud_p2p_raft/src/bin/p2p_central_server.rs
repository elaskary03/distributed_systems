use axum::{
    extract::{Multipart, Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use cloud_p2p_raft::crypto::{embed_lsb_rgba, extract_n_bytes};
use image::{imageops::FilterType, DynamicImage, GenericImageView, ImageOutputFormat};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio::fs;
use tower_http::cors::{Any, CorsLayer};

#[derive(Parser, Debug)]
#[command(author, version, about = "Central P2P image server (multi-owner)")]
struct Args {
    /// Port to listen on for centralized P2P HTTP
    #[arg(long, default_value_t = 10000)]
    port: u16,

    /// Base directory for user images
    #[arg(long, default_value = "user_images")]
    data_dir: String,
}

#[derive(Clone)]
struct AppState {
    base: PathBuf,
}

#[derive(Clone)]
struct OwnerPaths {
    original: PathBuf,
    encrypted: PathBuf,
    meta: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct Metadata {
    owner: String,
    permissions: HashMap<String, i64>,
    #[serde(default)]
    shared_passphrases: HashMap<String, String>,
    last_update_ns: u128,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
enum PendingUpdate {
    View { requester: String, delta: i64 },
    Perm { target_user: String, new_quota: i64 },
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct PendingRequest {
    viewer: String,
    requested_views: i64,
}

#[derive(Deserialize)]
struct RequestImage {
    requester: String,
    views: i64,
    #[serde(default)]
    passphrase: Option<String>,
}

#[derive(Deserialize)]
struct ConsumeViewReq {
    requester: String,
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
    let base = PathBuf::from(args.data_dir);
    fs::create_dir_all(&base).await?;

    let state = AppState { base };

    let app = Router::new()
        .route("/upload-image", post(upload_image))
        .route("/list-images", get(list_images))
        .route("/preview/:owner/:image_id", get(preview_image))
        .route("/full/:owner/:image_id", get(full_image))
        .route("/request-image/:owner/:image_id", post(request_image))
        .route("/approve-request/:owner/:image_id", post(approve_request))
        .route("/reject-request/:owner/:image_id", post(reject_request))
        .route("/consume-view/:owner/:image_id", post(consume_view))
        .route("/view-update/:owner/:image_id", post(view_update))
        .route(
            "/update-permissions/:owner/:image_id",
            post(update_permissions),
        )
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    println!("Central P2P server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
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
    let mut preview_bytes: Option<Vec<u8>> = None;

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
            "preview" => {
                if let Ok(bytes) = field.bytes().await {
                    preview_bytes = Some(bytes.to_vec());
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
        _ => return (StatusCode::BAD_REQUEST, "owner required").into_response(),
    };
    let stego_bytes = match image_bytes {
        Some(b) => b,
        None => return (StatusCode::BAD_REQUEST, "missing file").into_response(),
    };

    let owner_paths = match ensure_owner_paths(&st.base, &owner).await {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("create dir: {e}"),
            )
                .into_response();
        }
    };
    let base_perms: Option<HashMap<String, i64>> =
        permissions_raw.and_then(|s| serde_json::from_str(&s).ok());
    let mut meta = default_metadata(&owner, base_perms.clone());
    if let Some(p) = base_perms {
        meta.permissions = p;
    }

    let original_bytes = preview_bytes.clone().unwrap_or_else(|| stego_bytes.clone());

    if let Err(e) = save_image(&owner_paths.original, &image_id, &original_bytes).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("save original: {e}"),
        )
            .into_response();
    }

    if let Err(e) = save_preview_image(&owner_paths.encrypted, &image_id, &original_bytes).await {
        eprintln!("preview save failed for {}: {}", image_id, e);
    }

    meta.last_update_ns = now_nanos();
    if let Err(e) = save_metadata(&owner_paths.meta, &image_id, &meta).await {
        eprintln!("save metadata failed for {}: {}", image_id, e);
    }

    if let Err(e) = save_image(&owner_paths.encrypted, &image_id, &stego_bytes).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("save image: {e}"),
        )
            .into_response();
    }

    let _ = replay_pending(&owner_paths.meta, &image_id).await;

    Json(json!({"status":"ok"})).into_response()
}

async fn list_images(
    State(st): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    let requester = params.get("requester").cloned();
    let owner = params.get("owner").cloned().unwrap_or_default();
    if owner.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "owner required").into_response();
    }
    let is_owner = requester.as_ref().map(|r| r == &owner).unwrap_or(false);
    let mut images = Vec::new();
    let owner_dir = st.base.join(&owner);
    if let Ok(mut rd) = fs::read_dir(&owner_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if entry.path().extension().and_then(|s| s.to_str()) != Some("png") {
                continue;
            }
            let id = match entry
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
            {
                Some(v) => v,
                None => continue,
            };
            if id.ends_with("_preview") {
                continue;
            }
            if let Ok(mut meta) = load_metadata(&owner_dir, &id).await {
                if meta.owner.is_empty() {
                    meta.owner = owner.clone();
                }
                let remaining = requester
                    .as_ref()
                    .and_then(|r| meta.permissions.get(r))
                    .copied()
                    .unwrap_or(0);
                let mut obj = json!({
                    "id": id,
                    "owner": meta.owner,
                    "permissions": meta.permissions,
                    "remaining_views_for_requester": remaining,
                    "last_update_ns": meta.last_update_ns
                });
                if let Some(req) = requester.as_ref() {
                    if let Some(pw) = meta.shared_passphrases.get(req) {
                        obj["shared_passphrase"] = serde_json::Value::String(pw.clone());
                    }
                }
                if is_owner && meta.owner == owner {
                    if let Ok(reqs) = load_pending_requests(&owner_dir, &id).await {
                        obj["pending_requests"] = serde_json::to_value(reqs).unwrap_or(json!([]));
                    }
                }
                images.push(obj);
            }
        }
    }
    Json(json!({"status":"ok","images": images})).into_response()
}

async fn preview_image(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;

    if let Ok(bytes) = load_preview_image(&paths.encrypted, &image_id).await {
        return (StatusCode::OK, [("content-type", "image/png")], bytes).into_response();
    }

    match load_image(&paths.original, &image_id).await {
        Ok(orig) => match make_preview_bytes(&orig) {
            Ok(resized) => {
                return (StatusCode::OK, [("content-type", "image/png")], resized).into_response();
            }
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    }
}

async fn full_image(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    let requester = params.get("requester");
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;

    let requester = match requester {
        Some(r) => r.clone(),
        None => return (StatusCode::BAD_REQUEST, "missing requester").into_response(),
    };

    let mut meta = match load_metadata(&paths.meta, &image_id).await {
        Ok(m) => m,
        Err(_) => {
            let _ = enqueue_pending(
                &paths.meta,
                &image_id,
                PendingUpdate::View {
                    requester: requester.clone(),
                    delta: -1,
                },
            )
            .await;
            return (StatusCode::OK, "QUOTA_EXHAUSTED").into_response();
        }
    };

    if requester != meta.owner {
        let quota = meta.permissions.get(&requester).copied().unwrap_or(0);
        if quota <= 0 {
            return (StatusCode::OK, "QUOTA_EXHAUSTED").into_response();
        }
    }

    match load_image(&paths.encrypted, &image_id).await {
        Ok(bytes) => {
            let cd = format!("inline; filename=\"{}.png\"", image_id);
            (
                StatusCode::OK,
                [
                    ("content-type", "image/png".to_string()),
                    ("content-disposition", cd),
                    ("cache-control", "no-store".to_string()),
                ],
                bytes,
            )
                .into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn request_image(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Json(body): Json<RequestImage>,
) -> impl axum::response::IntoResponse {
    if body.requester.trim().is_empty() || body.views <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            "requester and positive views required",
        )
            .into_response();
    }
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;
    if load_image(&paths.encrypted, &image_id).await.is_err() {
        return (StatusCode::NOT_FOUND, "image not found").into_response();
    }

    match load_metadata(&paths.meta, &image_id).await {
        Ok(meta) => {
            if meta.permissions.get(&body.requester).copied().unwrap_or(0) > 0 {
                return (StatusCode::BAD_REQUEST, "already has quota").into_response();
            }
        }
        Err(_) => {}
    }

    let mut pending = load_pending_requests(&paths.meta, &image_id)
        .await
        .unwrap_or_default();
    if pending.iter().any(|r| r.viewer == body.requester) {
        return Json(json!({"status":"pending"})).into_response();
    }
    pending.push(PendingRequest {
        viewer: body.requester.clone(),
        requested_views: body.views,
    });
    if let Err(e) = save_pending_requests(&paths.meta, &image_id, &pending).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("save pending: {e}"),
        )
            .into_response();
    }

    Json(json!({"status":"queued","viewer": body.requester})).into_response()
}

async fn consume_view(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Json(body): Json<ConsumeViewReq>,
) -> impl axum::response::IntoResponse {
    if body.requester.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "requester required").into_response();
    }
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;
    if load_image(&paths.encrypted, &image_id).await.is_err() {
        return (StatusCode::NOT_FOUND, "image not found").into_response();
    }
    let mut meta = match load_metadata(&paths.meta, &image_id).await {
        Ok(m) => m,
        Err(_) => return (StatusCode::NOT_FOUND, "metadata not found").into_response(),
    };

    if body.requester != meta.owner {
        let entry = meta.permissions.entry(body.requester.clone()).or_insert(0);
        if *entry <= 0 {
            return (StatusCode::OK, "QUOTA_EXHAUSTED").into_response();
        }
        *entry -= 1;
    }
    meta.last_update_ns = now_nanos();
    ensure_owner_default_perm(&mut meta);
    let _ = save_metadata(&paths.meta, &image_id, &meta).await;
    let _ = replay_pending(&paths.meta, &image_id).await;

    Json(json!({"status":"ok","remaining": meta.permissions.get(&body.requester).copied().unwrap_or(0)})).into_response()
}

async fn approve_request(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Json(body): Json<RequestImage>,
) -> impl axum::response::IntoResponse {
    if body.requester.trim().is_empty() || body.views <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            "viewer and approved_views required",
        )
            .into_response();
    }
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;
    if load_image(&paths.encrypted, &image_id).await.is_err() {
        return (StatusCode::NOT_FOUND, "image not found").into_response();
    }
    let mut meta = load_metadata(&paths.meta, &image_id)
        .await
        .unwrap_or_else(|_| default_metadata(&owner, None));
    if meta.owner != owner {
        return (StatusCode::FORBIDDEN, "not owner").into_response();
    }

    let mut pending = load_pending_requests(&paths.meta, &image_id)
        .await
        .unwrap_or_default();
    if !pending.iter().any(|r| r.viewer == body.requester) {
        return (StatusCode::BAD_REQUEST, "no pending request").into_response();
    }

    let entry = meta.permissions.entry(body.requester.clone()).or_insert(0);
    *entry += body.views;
    meta.last_update_ns = now_nanos();
    ensure_owner_default_perm(&mut meta);
    if let Some(pw) = body.passphrase.as_ref() {
        if !pw.trim().is_empty() {
            meta.shared_passphrases
                .insert(body.requester.clone(), pw.trim().to_string());
        }
    }

    let _ = save_metadata(&paths.meta, &image_id, &meta).await;

    pending.retain(|r| r.viewer != body.requester);
    let _ = save_pending_requests(&paths.meta, &image_id, &pending).await;
    let _ = replay_pending(&paths.meta, &image_id).await;

    Json(json!({"status":"ok","permissions": meta.permissions})).into_response()
}

async fn reject_request(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Json(body): Json<RequestImage>,
) -> impl axum::response::IntoResponse {
    if body.requester.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "viewer required").into_response();
    }
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;
    if load_image(&paths.encrypted, &image_id).await.is_err() {
        return (StatusCode::NOT_FOUND, "image not found").into_response();
    }
    let meta = load_metadata(&paths.meta, &image_id)
        .await
        .unwrap_or_else(|_| default_metadata(&owner, None));
    if meta.owner != owner {
        return (StatusCode::FORBIDDEN, "not owner").into_response();
    }

    let mut pending = load_pending_requests(&paths.meta, &image_id)
        .await
        .unwrap_or_default();
    let orig = pending.len();
    pending.retain(|r| r.viewer != body.requester);
    if orig == pending.len() {
        return (StatusCode::BAD_REQUEST, "no pending request").into_response();
    }
    let _ = save_pending_requests(&paths.meta, &image_id, &pending).await;
    Json(json!({"status":"rejected","viewer": body.requester})).into_response()
}

async fn view_update(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Json(body): Json<UpdateViewReq>,
) -> impl IntoResponse {
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;
    let mut meta = load_metadata(&paths.meta, &image_id)
        .await
        .unwrap_or_else(|_| default_metadata(&owner, None));
    let entry = meta.permissions.entry(body.requester).or_insert(0);
    *entry = (*entry + body.delta).max(0);
    meta.last_update_ns = now_nanos();
    ensure_owner_default_perm(&mut meta);
    let _ = save_metadata(&paths.meta, &image_id, &meta).await;
    Json(json!({"status":"ok","permissions": meta.permissions}))
}

async fn update_permissions(
    State(st): State<AppState>,
    AxumPath((owner, image_id)): AxumPath<(String, String)>,
    Json(body): Json<UpdatePermissionsReq>,
) -> impl IntoResponse {
    let paths = resolve_owner_paths(&st.base, &owner, &image_id).await;
    let mut meta = load_metadata(&paths.meta, &image_id)
        .await
        .unwrap_or_else(|_| default_metadata(&owner, None));
    meta.permissions.insert(body.target_user, body.new_quota);
    meta.last_update_ns = now_nanos();
    ensure_owner_default_perm(&mut meta);
    let _ = save_metadata(&paths.meta, &image_id, &meta).await;
    Json(json!({"status":"ok","permissions": meta.permissions}))
}

async fn find_owner_paths(base: &PathBuf, image_id: &str) -> Option<OwnerPaths> {
    if let Ok(mut rd) = fs::read_dir(base).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if !entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let owner_dir = entry.path();
            let candidate = owner_dir.join(format!("{}.png", image_id));
            if fs::metadata(&candidate).await.is_ok() {
                return Some(OwnerPaths {
                    original: owner_dir.join("original"),
                    encrypted: owner_dir.clone(),
                    meta: owner_dir.clone(),
                });
            }
        }
    }
    None
}

async fn resolve_owner_paths(base: &PathBuf, owner: &str, image_id: &str) -> OwnerPaths {
    if let Some(p) = find_owner_paths(base, image_id).await {
        return p;
    }
    ensure_owner_paths(base, owner)
        .await
        .unwrap_or_else(|_| OwnerPaths {
            original: base.join(owner).join("original"),
            encrypted: base.join(owner),
            meta: base.join(owner),
        })
}

async fn load_metadata(root: &PathBuf, id: &str) -> Result<Metadata, anyhow::Error> {
    let path = root.join(format!("{}.json", id));
    if let Ok(data) = fs::read(&path).await {
        let mut meta: Metadata = serde_json::from_slice(&data)?;
        let owner_hint = root
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        if meta.owner.is_empty() && !owner_hint.is_empty() {
            meta.owner = owner_hint;
        }
        ensure_owner_default_perm(&mut meta);
        return Ok(meta);
    }

    if let Ok(img) = load_image(root, id).await {
        if let Ok(mut meta) = extract_metadata_from_png(&img, "") {
            ensure_owner_default_perm(&mut meta);
            return Ok(meta);
        }
    }
    anyhow::bail!("metadata not found")
}

async fn save_metadata(root: &PathBuf, id: &str, meta: &Metadata) -> Result<(), anyhow::Error> {
    fs::create_dir_all(root).await.ok();
    let path = root.join(format!("{}.json", id));
    let data = serde_json::to_vec_pretty(meta)?;
    fs::write(path, data).await?;
    Ok(())
}

async fn load_image(root: &PathBuf, id: &str) -> Result<Vec<u8>, anyhow::Error> {
    let path = root.join(format!("{}.png", id));
    Ok(fs::read(path).await?)
}

async fn load_preview_image(root: &PathBuf, id: &str) -> Result<Vec<u8>, anyhow::Error> {
    let path = root.join(format!("{}_preview.png", id));
    Ok(fs::read(path).await?)
}

async fn save_image(root: &PathBuf, id: &str, bytes: &[u8]) -> Result<(), anyhow::Error> {
    let path = root.join(format!("{}.png", id));
    fs::write(path, bytes).await?;
    Ok(())
}

async fn enqueue_pending(
    root: &PathBuf,
    id: &str,
    upd: PendingUpdate,
) -> Result<(), anyhow::Error> {
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

    let mut meta = load_metadata(root, id).await.unwrap_or_else(|_| {
        let owner = root
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        default_metadata(&owner, None)
    });
    for upd in updates {
        match upd {
            PendingUpdate::View { requester, delta } => {
                let entry = meta.permissions.entry(requester).or_insert(0);
                *entry = (*entry + delta).max(0);
            }
            PendingUpdate::Perm {
                target_user,
                new_quota,
            } => {
                meta.permissions.insert(target_user, new_quota);
            }
        }
    }
    meta.last_update_ns = now_nanos();
    let _ = save_metadata(root, id, &meta).await;
    let _ = fs::remove_file(&path).await;
    Ok(())
}

async fn load_pending_requests(
    root: &PathBuf,
    id: &str,
) -> Result<Vec<PendingRequest>, anyhow::Error> {
    let dir = root.join("pending_requests");
    let path = dir.join(format!("{}.json", id));
    if let Ok(data) = fs::read(&path).await {
        let reqs: Vec<PendingRequest> = serde_json::from_slice(&data).unwrap_or_default();
        Ok(reqs)
    } else {
        Ok(Vec::new())
    }
}

async fn save_pending_requests(
    root: &PathBuf,
    id: &str,
    reqs: &[PendingRequest],
) -> Result<(), anyhow::Error> {
    let dir = root.join("pending_requests");
    fs::create_dir_all(&dir).await.ok();
    let path = dir.join(format!("{}.json", id));
    if reqs.is_empty() {
        let _ = fs::remove_file(&path).await;
        return Ok(());
    }
    let data = serde_json::to_vec_pretty(reqs)?;
    fs::write(path, data).await?;
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

fn default_metadata(owner: &str, base: Option<HashMap<String, i64>>) -> Metadata {
    let mut permissions = base.unwrap_or_default();
    Metadata {
        owner: owner.to_string(),
        permissions,
        shared_passphrases: HashMap::new(),
        last_update_ns: now_nanos(),
    }
}

fn ensure_owner_default_perm(meta: &mut Metadata) {
    meta.permissions.remove(&meta.owner);
}

fn embed_metadata_into_png(img_bytes: &[u8], meta: &Metadata) -> anyhow::Result<Vec<u8>> {
    use image::ImageOutputFormat;
    let rgba = image::load_from_memory(img_bytes)?.to_rgba8();
    let json = serde_json::to_vec(meta)?;
    let mut blob = Vec::with_capacity(4 + json.len());
    blob.extend_from_slice(&(json.len() as u32).to_be_bytes());
    blob.extend_from_slice(&json);
    let stego = embed_lsb_rgba(&rgba, &blob)?;
    let mut out = Vec::new();
    stego.write_to(&mut std::io::Cursor::new(&mut out), ImageOutputFormat::Png)?;
    Ok(out)
}

fn extract_metadata_from_png(img_bytes: &[u8], owner_hint: &str) -> anyhow::Result<Metadata> {
    let rgba = image::load_from_memory(img_bytes)?.to_rgba8();
    let len_bytes = extract_n_bytes(&rgba, 4)?;
    let len = u32::from_be_bytes(len_bytes.as_slice().try_into().unwrap()) as usize;
    let blob = extract_n_bytes(&rgba, 4 + len)?;
    let meta: Metadata = serde_json::from_slice(&blob[4..])?;
    if meta.owner.is_empty() && !owner_hint.is_empty() {
        return Ok(Metadata {
            owner: owner_hint.to_string(),
            ..meta
        });
    }
    Ok(meta)
}

fn make_preview_bytes(img_bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    let img = image::load_from_memory(img_bytes)?;
    let preview = resize_to_width(img, 200);
    let mut buf = Vec::new();
    preview.write_to(&mut std::io::Cursor::new(&mut buf), ImageOutputFormat::Png)?;
    Ok(buf)
}

async fn save_preview_image(root: &PathBuf, id: &str, img_bytes: &[u8]) -> anyhow::Result<()> {
    let preview = make_preview_bytes(img_bytes)?;
    let path = root.join(format!("{}_preview.png", id));
    fs::write(path, preview).await?;
    Ok(())
}

async fn ensure_owner_paths(base: &Path, owner: &str) -> Result<OwnerPaths, anyhow::Error> {
    let owner_dir = base.join(owner);
    let original = owner_dir.join("original");
    let encrypted = owner_dir.clone();
    let meta = owner_dir.clone();
    fs::create_dir_all(&original).await?;
    fs::create_dir_all(&encrypted).await?;
    fs::create_dir_all(meta.join("pending_updates")).await.ok();
    fs::create_dir_all(meta.join("pending_requests")).await.ok();
    Ok(OwnerPaths {
        original,
        encrypted,
        meta,
    })
}
