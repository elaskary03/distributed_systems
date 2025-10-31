use axum::{
    extract::{Multipart, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs as tokio_fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tower_http::services::ServeDir;

// ===== crypto + stego =====
use chacha20poly1305::{aead::Aead, aead::KeyInit, ChaCha20Poly1305, Key, Nonce};
use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha256};
use image::{DynamicImage, RgbaImage};

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud P2P GUI (talks to the existing Proxy)")]
struct Args {
    /// HTTP listen address for the GUI web app
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,

    /// Proxy address to talk to (must be the proxy's --listen)
    #[arg(long, default_value = "127.0.0.1:9100")]
    proxy_addr: String,
}

#[derive(Clone)]
struct AppState {
    proxy_addr: Arc<String>,
    uploads_dir: Arc<PathBuf>,
    stego_dir: Arc<PathBuf>,
}

/* =========================
   API payloads
   ========================= */
#[derive(Deserialize)]
struct RegisterReq {
    user: String,
    ip: String,
}

#[derive(Deserialize)]
struct UnregisterReq {
    user: String,
}

#[derive(Serialize)]
struct UploadResp {
    image_id: String,
    original_path: String,
    stego_path: String,
    ciphertext_sha256: String,
    bytes_embedded: usize,
}

/* =========================
   main
   ========================= */
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Ensure storage dirs exist
    let uploads_dir = PathBuf::from("uploads");
    let stego_dir = PathBuf::from("stego");
    fs::create_dir_all(&uploads_dir).ok();
    fs::create_dir_all(&stego_dir).ok();

    let state = AppState {
        proxy_addr: Arc::new(args.proxy_addr),
        uploads_dir: Arc::new(uploads_dir),
        stego_dir: Arc::new(stego_dir),
    };

    // Static file serving for downloads
    let files_router = Router::new()
        .nest_service(
            "/uploads",
            ServeDir::new("uploads").append_index_html_on_directories(false),
        )
        .nest_service(
            "/stego",
            ServeDir::new("stego").append_index_html_on_directories(false),
        );

    let app = Router::new()
        // UI
        .route("/", get(ui))
        // API
        .route("/api/register", post(api_register))
        .route("/api/unregister", post(api_unregister))
        .route("/api/leader", get(api_leader))
        .route("/api/users", get(api_users))
        .route("/api/list", get(api_list))
        .route("/api/send", post(api_send))
        // NEW: upload
        .route("/api/upload", post(api_upload))
        // files
        .nest("/files", files_router)
        .with_state(state);

    let addr: SocketAddr = args.listen.parse()?;
    println!("🖥️  GUI available at http://{}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/* =========================
   UI
   ========================= */
async fn ui() -> impl IntoResponse {
    const PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Cloud P2P GUI</title>
<style>
  :root { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; }
  body { margin: 24px; }
  h1 { margin-bottom: 8px; }
  .card { padding: 16px; border: 1px solid #ddd; border-radius: 12px; margin-bottom: 16px; }
  .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  input, button, select { padding: 8px 10px; border-radius: 8px; border: 1px solid #ccc; }
  button { cursor: pointer; }
  pre { background: #f7f7f7; padding: 12px; border-radius: 10px; overflow:auto; max-height: 300px;}
  .muted { color:#666; font-size: 12px; }
  label { display:block; margin: 4px 0; }
</style>
</head>
<body>
  <h1>Cloud P2P – Client GUI</h1>

  <div class="card">
    <h3>Leader</h3>
    <div class="row">
      <button onclick="fetchText('/api/leader','#leader')">Get Leader</button>
      <button onclick="fetchText('/api/list','#log')">Show Log</button>
      <button onclick="fetchText('/api/users','#users')">Show Users</button>
    </div>
    <h4>Leader:</h4>
    <pre id="leader">(click "Get Leader")</pre>
  </div>

  <div class="card">
    <h3>Register User</h3>
    <div class="row">
      <input id="reg-user" placeholder="user (e.g., alice)" />
      <input id="reg-ip" placeholder="ip (e.g., 192.168.1.100)" />
      <button onclick="registerUser()">Register</button>
    </div>
    <pre id="reg-resp"></pre>
  </div>

  <div class="card">
    <h3>Unregister User</h3>
    <div class="row">
      <input id="unreg-user" placeholder="user (e.g., alice)" />
      <button onclick="unregisterUser()">Unregister</button>
    </div>
    <pre id="unreg-resp"></pre>
  </div>

  <div class="card">
    <h3>Users</h3>
    <pre id="users">(click "Show Users")</pre>
  </div>

  <div class="card">
    <h3>Log</h3>
    <pre id="log">(click "Show Log")</pre>
  </div>

  <div class="card">
    <h3>Upload & Stego-Encrypt Image</h3>
    <form id="uploadForm">
      <label>Image file: <input type="file" id="file" name="file" accept="image/*" required /></label>
      <label>Passphrase (optional – default random): <input id="pass" name="passphrase" type="password" /></label>
      <button type="submit">Upload & Encrypt</button>
    </form>
    <div class="muted">We encrypt the image with ChaCha20-Poly1305 and hide the ciphertext in the image's least significant bits. The stego PNG is downloadable below.</div>
    <pre id="upload-resp"></pre>
  </div>

<script>
async function fetchText(url, sel) {
  const r = await fetch(url);
  const t = await r.text();
  document.querySelector(sel).textContent = t;
}
async function registerUser() {
  const user = document.querySelector('#reg-user').value.trim();
  const ip = document.querySelector('#reg-ip').value.trim();
  const r = await fetch('/api/register', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({user, ip})});
  const t = await r.text();
  document.querySelector('#reg-resp').textContent = t;
}
async function unregisterUser() {
  const user = document.querySelector('#unreg-user').value.trim();
  const r = await fetch('/api/unregister', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({user})});
  const t = await r.text();
  document.querySelector('#unreg-resp').textContent = t;
}
document.querySelector('#uploadForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData();
  const f = document.querySelector('#file').files[0];
  const pass = document.querySelector('#pass').value;
  fd.append('file', f);
  fd.append('passphrase', pass);
  const r = await fetch('/api/upload', { method:'POST', body: fd });
  const txt = await r.text();
  try {
    const j = JSON.parse(txt);
    document.querySelector('#upload-resp').textContent =
      JSON.stringify(j, null, 2) +
      "\nDownload stego: " + (j.stego_path || "");
  } catch {
    document.querySelector('#upload-resp').textContent = txt;
  }
});
</script>
</body>
</html>
"#;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "text/html; charset=utf-8".parse().unwrap());
    (StatusCode::OK, headers, Html(PAGE))
}

/* =========================
   API handlers (existing)
   ========================= */
#[derive(Deserialize)]
struct SendBody(String);

async fn api_register(
    State(st): State<AppState>,
    Json(payload): Json<RegisterReq>,
) -> impl IntoResponse {
    if payload.user.trim().is_empty() || payload.ip.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "user and ip are required").into_response();
    }
    let line = format!("REGISTER {} {}", payload.user, payload.ip);
    proxy_send_oneline(&st.proxy_addr, &line).await.into_response()
}

async fn api_unregister(
    State(st): State<AppState>,
    Json(payload): Json<UnregisterReq>,
) -> impl IntoResponse {
    if payload.user.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "user is required").into_response();
    }
    let line = format!("UNREGISTER {}", payload.user);
    proxy_send_oneline(&st.proxy_addr, &line).await.into_response()
}

async fn api_leader(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "LEADER").await.into_response()
}
async fn api_users(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "SHOW_USERS").await.into_response()
}
async fn api_list(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "LIST").await.into_response()
}
async fn api_send(State(st): State<AppState>, body: String) -> impl IntoResponse {
    let line = body.trim();
    if line.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty command").into_response();
    }
    if line == "LEADER" || line == "LIST" || line == "SHOW_USERS" {
        proxy_send_multiline(&st.proxy_addr, line).await.into_response()
    } else {
        proxy_send_oneline(&st.proxy_addr, line).await.into_response()
    }
}

/* =========================
   NEW: Upload handler
   ========================= */
async fn api_upload(
    State(st): State<AppState>,
    mut mp: Multipart,
) -> impl IntoResponse {
    // Read fields
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut filename: String = "upload".to_string();
    let mut passphrase: Option<String> = None;

    while let Ok(Some(field)) = mp.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            if let Some(fname) = field.file_name() {
                filename = fname.to_string();
            }
            match field.bytes().await {
                Ok(b) if !b.is_empty() => file_bytes = Some(b.to_vec()),
                _ => return (StatusCode::BAD_REQUEST, "empty file").into_response(),
            }
        } else if name == "passphrase" {
            passphrase = Some(field.text().await.unwrap_or_default());
        }
    }

    let bytes = match file_bytes {
        Some(b) => b,
        None => return (StatusCode::BAD_REQUEST, "missing file").into_response(),
    };

    // Persist original
    let image_id = format!("img-{}", now_nanos());
    let original_path = st.uploads_dir.join(format!("{}-{}", image_id, sanitize(&filename)));
    if let Err(e) = tokio_fs::write(&original_path, &bytes).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("write original: {e}")).into_response();
    }

    // Load image
    let rgba = match image::load_from_memory(&bytes) {
        Ok(i) => i.to_rgba8(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid image: {e}")).into_response(),
    };

    // Encrypt raw bytes with ChaCha20-Poly1305
    let pass = passphrase.unwrap_or_default();
    let key = derive_key(pass.as_bytes());
    let nonce = rand_nonce();
    let cipher = ChaCha20Poly1305::new(&key);
    let ciphertext = match cipher.encrypt(&nonce, bytes.as_ref()) {
        Ok(ct) => ct,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("encrypt: {e}")).into_response(),
    };

    // Prepare data to embed: [nonce(12) | len_u32 | ciphertext]
    let mut embed_data = Vec::with_capacity(12 + 4 + ciphertext.len());
    embed_data.extend_from_slice(nonce.as_ref()); // non-deprecated
    embed_data.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
    embed_data.extend_from_slice(&ciphertext);

    // Capacity check: 1 LSB per RGBA channel (4 bits/pixel) → capacity_bytes = (w*h*4)/8
    let (w, h) = (rgba.width() as usize, rgba.height() as usize);
    let capacity_bits = w * h * 4;
    let needed_bits = embed_data.len() * 8;
    if needed_bits > capacity_bits {
        let msg = format!(
            "image too small for payload: need {} bits ({} bytes), capacity {} bits (~{} bytes). Use a larger image.",
            needed_bits, embed_data.len(), capacity_bits, capacity_bits/8
        );
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }

    // Embed into LSBs
    let stego = match embed_lsb_rgba(&rgba, &embed_data) {
        Ok(img) => img,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("embed: {e}")).into_response(),
    };

    // Save stego as PNG
    let stego_name = format!("{}-stego.png", image_id);
    let stego_path = st.stego_dir.join(&stego_name);
    if let Err(e) = stego.save(&stego_path) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("save stego: {e}")).into_response();
    }

    // Compute SHA256 of ciphertext for metadata/logging
    let ct_sha = {
        let mut hasher = Sha256::new();
        hasher.update(&ciphertext);
        hex::encode(hasher.finalize())
    };

    // Log metadata via proxy using SUBMIT (non-breaking)
    let _ = proxy_send_oneline(
        &st.proxy_addr,
        &format!("SUBMIT {} ENCRYPT_IMAGE {} {}", next_op_id(), image_id, ct_sha),
    )
    .await;

    // Build response (paths are under /files/...)
    let resp = UploadResp {
        image_id,
        original_path: format!("/files/uploads/{}", original_path.file_name().unwrap().to_string_lossy()),
        stego_path: format!("/files/stego/{}", stego_name),
        ciphertext_sha256: ct_sha,
        bytes_embedded: embed_data.len(),
    };

    (StatusCode::OK, Json(resp)).into_response()
}

/* =========================
   Proxy bridge helpers
   ========================= */
async fn proxy_send_oneline(proxy_addr: &str, cmd_line: &str) -> (StatusCode, String) {
    match talk_once(proxy_addr, cmd_line).await {
        Ok(s) => (StatusCode::OK, s),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("ERR {e}")),
    }
}

async fn proxy_send_multiline(proxy_addr: &str, cmd_line: &str) -> (StatusCode, String) {
    match read_multiline(proxy_addr, cmd_line).await {
        Ok(s) => (StatusCode::OK, s),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("ERR {e}")),
    }
}

async fn talk_once(proxy_addr: &str, cmd_line: &str) -> anyhow::Result<String> {
    let mut s = TcpStream::connect(proxy_addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // proxy banner
    let mut tmp = String::new();
    reader.read_line(&mut tmp).await?;
    tmp.clear();
    reader.read_line(&mut tmp).await?;

    // command
    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    // one line
    let mut resp = String::new();
    reader.read_line(&mut resp).await?;
    Ok(resp)
}

/// Wait for first line (≤1s), then read until 150ms idle.
async fn read_multiline(proxy_addr: &str, cmd_line: &str) -> anyhow::Result<String> {
    let mut s = TcpStream::connect(proxy_addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // banner
    let mut tmp = String::new();
    reader.read_line(&mut tmp).await?;
    tmp.clear();
    reader.read_line(&mut tmp).await?;

    // send
    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut out = String::new();

    // ensure first line
    let mut first = String::new();
    match timeout(Duration::from_secs(1), reader.read_line(&mut first)).await {
        Ok(Ok(n)) if n > 0 => {
            out.push_str(&first);
            if cmd_line == "LEADER" {
                return Ok(out);
            }
        }
        _ => anyhow::bail!("empty reply"),
    }

    // gather the rest until idle
    loop {
        let mut buf = String::new();
        tokio::select! {
            n = reader.read_line(&mut buf) => {
                let n = n?;
                if n == 0 { break; }
                out.push_str(&buf);
            }
            _ = sleep(Duration::from_millis(150)) => { break; }
        }
    }

    Ok(out)
}

/* =========================
   Stego helpers
   ========================= */

fn derive_key(pass: &[u8]) -> Key {
    // SHA-256(passphrase) → 32 bytes
    let mut h = Sha256::new();
    h.update(pass);
    let out = h.finalize();
    // Key is a type alias (no generic param)
    *Key::from_slice(&out)
}

fn rand_nonce() -> Nonce {
    // 12-byte nonce
    let mut n = [0u8; 12];
    OsRng.fill_bytes(&mut n);
    *Nonce::from_slice(&n)
}

fn embed_lsb_rgba(img: &RgbaImage, data: &[u8]) -> anyhow::Result<DynamicImage> {
    // Embed 1 bit per channel (RGBA) in raster order.
    let mut out = img.clone();
    let mut bit_idx = 0usize;

    for y in 0..out.height() {
        for x in 0..out.width() {
            let mut px = *out.get_pixel(x, y); // copy value, not a ref
            for ch in 0..4 {
                if bit_idx >= data.len() * 8 {
                    out.put_pixel(x, y, px);
                    return Ok(DynamicImage::ImageRgba8(out));
                }
                let byte = data[bit_idx / 8];
                let bit = (byte >> (7 - (bit_idx % 8))) & 1;
                px[ch] = (px[ch] & 0xFE) | bit; // set LSB
                bit_idx += 1;
            }
            out.put_pixel(x, y, px);
        }
    }

    if bit_idx < data.len() * 8 {
        anyhow::bail!("not enough capacity (unexpected; capacity was checked)");
    }
    Ok(DynamicImage::ImageRgba8(out))
}

/* =========================
   small utils
   ========================= */
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn now_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn next_op_id() -> String {
    let mut r = [0u8; 4];
    OsRng.fill_bytes(&mut r);
    format!("gui-{}-{:02x}{:02x}{:02x}{:02x}", now_nanos(), r[0], r[1], r[2], r[3])
}
