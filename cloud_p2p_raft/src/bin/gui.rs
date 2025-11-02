use axum::{
    extract::{Multipart, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{fs, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    fs as tokio_fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tower_http::services::ServeDir;
use rand_core::{OsRng, RngCore};

// 🔐 client-side decryption uses the same helpers as the cluster
use cloud_p2p_raft::crypto::{decrypt_bytes, extract_payload};

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud P2P GUI (talks to the existing Proxy)")]
struct Args {
    /// HTTP listen address for the GUI web app
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,

    /// Proxy address to talk to (must be the proxy's --listen)
    #[arg(long, default_value = "127.0.0.1:9100")]
    proxy_addr: String,

    /// Public base URL for this GUI (reachable by nodes), e.g. http://192.168.8.41:8080
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    public_base: String,
}

#[derive(Clone)]
struct AppState {
    proxy_addr: Arc<String>,
    uploads_dir: Arc<PathBuf>,
    stego_dir: Arc<PathBuf>,
    public_base: Arc<String>, // NEW
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
    status: String,
}

#[derive(Serialize)]
struct FindStegoResp { stego_path: String }

/* =========================
   main
   ========================= */
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let uploads_dir = PathBuf::from("uploads");
    let stego_dir = PathBuf::from("stego");
    fs::create_dir_all(&uploads_dir).ok();
    fs::create_dir_all(&stego_dir).ok();

    let state = AppState {
        proxy_addr: Arc::new(args.proxy_addr),
        uploads_dir: Arc::new(uploads_dir),
        stego_dir: Arc::new(stego_dir),
        public_base: Arc::new(args.public_base), // NEW
    };

    // Serve both /files/uploads/* and /files/stego/* so the browser can fetch them
    let files_router = Router::new()
        .nest_service("/uploads", ServeDir::new("uploads").append_index_html_on_directories(false))
        .nest_service("/stego",   ServeDir::new("stego").append_index_html_on_directories(false));

    let app = Router::new()
        .route("/", get(ui))
        .route("/api/register", post(api_register))
        .route("/api/unregister", post(api_unregister))
        .route("/api/leader", get(api_leader))
        .route("/api/users", get(api_users))
        .route("/api/list", get(api_list))
        .route("/api/upload", post(api_upload))
        .route("/api/decrypt", post(api_decrypt))         // client-side decrypt
        .route("/api/find-stego", get(api_find_stego))    // poll for saved stego
        .route("/api/push-stego", post(api_push_stego))   // NEW: nodes POST stego here
        .nest("/files", files_router)
        .with_state(state);

    let addr: SocketAddr = args.listen.parse()?;
    println!("🖥️  GUI available at http://{}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/* =========================
   UI (HTML)
   ========================= */
async fn ui() -> impl IntoResponse {
    const PAGE: &str = r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Cloud P2P GUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{
      --bg: #f7f9fc; --text: #1f2937; --muted: #6b7280; --card: #ffffff; --border: #e5e7eb;
      --accent: #2563eb; --accent-700: #1d4ed8; --ok: #16a34a; --warn: #b45309;
    }
    *{ box-sizing: border-box; } body{ font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: var(--bg); color: var(--text); margin: 0; }
    .container{ max-width: 1120px; margin: 0 auto; padding: 24px 16px 56px; }
    h1{ margin:0 0 6px; font-size: 28px; line-height: 1.2; } .sub{ color: var(--muted); margin-bottom: 18px; }
    .grid{ display:grid; gap:16px; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); align-items:start; }
    .grid-2-1{ display:grid; gap:16px; grid-template-columns:2fr 1fr; } @media (max-width:960px){ .grid-2-1{ grid-template-columns:1fr; } }
    section{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:16px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
    section h2{ margin:0 0 10px; font-size:18px; display:flex; align-items:center; gap:8px; }
    .badge{ font-size:12px; color:#1e40af; background:#e0e7ff; border:1px solid #c7d2fe; padding:2px 8px; border-radius:999px; }
    label{ display:block; font-size:13px; color:var(--muted); margin-bottom:6px; }
    input[type="text"], input[type="password"], input[type="file"]{ width:100%; padding:10px 12px; border-radius:10px; border:1px solid var(--border); background:#fff; color:var(--text); }
    input:focus{ outline:none; border-color:#c4d0ff; box-shadow:0 0 0 3px rgba(37,99,235,.18); }
    .row{ display:grid; grid-template-columns:1fr; gap:12px; } @media (min-width:720px){ .row{ grid-template-columns:1fr 1fr; } }
    .btns{ display:flex; gap:8px; flex-wrap:wrap; } button{ cursor:pointer; border:1px solid #d1d5db; background:#f9fafb; color:#111827; padding:8px 12px; border-radius:10px; font-weight:600; }
    button:hover{ background:#f3f4f6; } .btn-accent{ background:var(--accent); border-color:var(--accent); color:#fff; } .btn-accent:hover{ background:var(--accent-700); }
    .btn-ok{ color:#fff; background:var(--ok); border-color:var(--ok); } .btn-warn{ color:#fff; background:var(--warn); border-color:var(--warn); }
    .out{ margin-top:10px; background:#f8fafc; border:1px solid var(--border); border-radius:10px; padding:10px 12px; min-height:44px; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; font-size:13px; white-space:pre-wrap; overflow:auto; max-height:220px; }
    .hint{ color:var(--muted); font-size:12px; margin-top:8px; } .pill{ display:inline-flex; align-items:center; gap:6px; padding:4px 8px; border-radius:999px; font-size:12px; border:1px solid var(--border); background:#fff; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Cloud P2P GUI</h1>
    <div class="sub">Proxy-backed UI for your Raft cluster. Upload an image to encrypt & embed via the cluster. Manage users and inspect state.</div>

    <div class="grid-2-1">
      <section>
        <h2>🖼️ Upload & Encrypt <span class="badge">ENCRYPT_ON_CLOUD</span></h2>
        <form id="uploadForm">
          <div class="row">
            <div><label>Image file</label><input type="file" name="file" required></div>
            <div><label>Passphrase</label><input type="password" name="passphrase" placeholder="required by proxy"></div>
          </div>
          <div style="margin-top:12px" class="btns"><button type="submit" class="btn-accent">Upload & Encrypt</button></div>
        </form>
        <div id="uploadOut" class="out"></div>
        <div class="hint">Encrypted image will be saved under <span class="pill">stego/</span> on this GUI host. The page will auto-download it when ready.</div>
      </section>

      <section>
        <h2>👑 Leader</h2>
        <div class="btns"><button id="leaderBtn">LEADER</button></div>
        <div id="leaderOut" class="out"></div>
      </section>
    </div>

    <div class="grid">
      <section>
        <h2>👤 Users — Register / Unregister</h2>
        <form id="regForm" style="margin-bottom:8px">
          <div class="row">
            <div><label>User</label><input name="user" type="text" placeholder="alice" required></div>
            <div><label>IP</label><input name="ip" type="text" placeholder="10.0.0.1" required></div>
          </div>
          <div style="margin-top:8px" class="btns">
            <button type="submit" class="btn-ok">REGISTER</button>
            <button type="button" id="unregBtn" class="btn-warn">UNREGISTER</button>
          </div>
        </form>
        <div id="regOut" class="out"></div>
      </section>

      <section>
        <h2>📜 Inspect</h2>
        <div class="btns" style="margin-bottom:8px">
          <button id="usersBtn">SHOW_USERS</button>
          <button id="listBtn">LIST</button>
        </div>
        <div><label>SHOW_USERS Output</label><div id="usersOut" class="out"></div></div>
        <div style="margin-top:10px"><label>LIST Output</label><div id="listOut" class="out"></div></div>
      </section>
    </div>

    <div class="grid">
      <section>
        <h2>🗝️ Decrypt Local Image <span class="badge">client-side</span></h2>
        <form id="decryptForm">
          <div class="row">
            <div><label>Stego image (PNG/JPEG)</label><input type="file" name="file" accept="image/png,image/jpeg" required></div>
            <div><label>Passphrase</label><input type="password" name="passphrase" placeholder="the same key you used" required></div>
          </div>
          <div style="margin-top:12px" class="btns"><button type="submit">Decrypt & Download Payload</button></div>
        </form>
        <div id="decryptOut" class="out"></div>
        <div class="hint">Runs on the GUI server (not the cluster). If the payload is an image, it downloads with the right extension; otherwise falls back to <span class="pill">decrypted.bin</span>.</div>
      </section>
    </div>
  </div>

  <script>
    const $ = sel => document.querySelector(sel);
    const text = (id, s) => ($(id).textContent = s);

    async function pollFindStego(imageId, timeoutMs = 20000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const res = await fetch('/api/find-stego?image_id=' + encodeURIComponent(imageId));
        if (res.ok) { const json = await res.json(); return json.stego_path; }
        await new Promise(r => setTimeout(r, 600));
      }
      return null;
    }

    $('#uploadForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: fd });
        if (!res.ok) { text('#uploadOut', await res.text()); return; }
        const json = await res.json();
        text('#uploadOut', JSON.stringify(json, null, 2));

        const imageId = json.image_id;
        const stegoPath = await pollFindStego(imageId);
        if (stegoPath) {
          const a = document.createElement('a');
          a.href = stegoPath;
          a.download = stegoPath.split('/').pop() || `stego-${imageId}.png`;
          a.click();
          text('#uploadOut', JSON.stringify(json, null, 2) + `\n✅ Stego downloaded.`);
        } else {
          text('#uploadOut', JSON.stringify(json, null, 2) + `\n⚠️ Stego not found yet for ${imageId}`);
        }
      } catch (err) { text('#uploadOut', String(err)); }
    });

    $('#regForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const f = new FormData(e.target);
      const body = { user: f.get('user'), ip: f.get('ip') };
      try { const res = await fetch('/api/register', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) }); text('#regOut', await res.text()); }
      catch (err) { text('#regOut', String(err)); }
    });

    $('#unregBtn').addEventListener('click', async () => {
      const f = new FormData($('#regForm')); const user = f.get('user');
      try { const res = await fetch('/api/unregister', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ user }) }); text('#regOut', await res.text()); }
      catch (err) { text('#regOut', String(err)); }
    });

    $('#usersBtn').addEventListener('click', async () => { try { text('#usersOut', await (await fetch('/api/users')).text()); } catch (err) { text('#usersOut', String(err)); } });
    $('#listBtn').addEventListener('click', async () => { try { text('#listOut', await (await fetch('/api/list')).text()); } catch (err) { text('#listOut', String(err)); } });
    $('#leaderBtn').addEventListener('click', async () => { try { text('#leaderOut', await (await fetch('/api/leader')).text()); } catch (err) { text('#leaderOut', String(err)); } });

    $('#decryptForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        const res = await fetch('/api/decrypt', { method: 'POST', body: fd });
        if (!res.ok) { text('#decryptOut', await res.text()); return; }
        const blob = await res.blob();
        const cd = res.headers.get('Content-Disposition') || '';
        let fname = 'decrypted.bin';
        const m = cd.match(/filename="([^"]+)"/i); if (m) fname = m[1];
        const url = URL.createObjectURL(blob); const a = document.createElement('a');
        a.href = url; a.download = fname; a.click(); URL.revokeObjectURL(url);
        text('#decryptOut', '✅ Decryption successful — file downloaded as ' + fname);
      } catch (err) { text('#decryptOut', '❌ ' + err); }
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
   API handlers
   ========================= */

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

/* =========================
   Upload handler (encrypt-on-cloud trigger)
   ========================= */
async fn api_upload(
    State(st): State<AppState>,
    mut mp: Multipart,
) -> impl IntoResponse {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut filename: String = "upload".to_string();
    let mut passphrase: Option<String> = None;

    while let Ok(Some(field)) = mp.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            if let Some(fname) = field.file_name() { filename = fname.to_string(); }
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

    let image_id = format!("img-{}", now_nanos());
    let original_path = st.uploads_dir.join(format!("{}-{}", image_id, sanitize(&filename)));

    if let Err(e) = tokio_fs::write(&original_path, &bytes).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("write original: {e}")).into_response();
    }

    // Ask the proxy to coordinate the cluster-side encryption/stego.
    // Include a callback URL that nodes will POST the stego PNG to.
    let pass = passphrase.unwrap_or_default();
    let callback = format!("{}/api/push-stego", *st.public_base);
    let cmd = format!("ENCRYPT_ON_CLOUD {} {} {}", image_id, pass, callback);
    let (status, resp) = proxy_send_oneline(&st.proxy_addr, &cmd).await;

    let resp = UploadResp {
        image_id,
        original_path: format!("/files/uploads/{}", original_path.file_name().unwrap().to_string_lossy()),
        status: resp,
    };

    (status, Json(resp)).into_response()
}

/* =========================
   Nodes push stego here -> GUI saves to stego/
   ========================= */
async fn api_push_stego(
    State(st): State<AppState>,
    mut mp: Multipart,
) -> impl IntoResponse {
    let mut image_id = String::new();
    let mut sha = String::new();
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut file_name: String = String::new();

    while let Ok(Some(field)) = mp.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "image_id" => image_id = field.text().await.unwrap_or_default(),
            "sha256"   => sha = field.text().await.unwrap_or_default(),
            "file"     => {
                if let Some(fname) = field.file_name() { file_name = fname.to_string(); }
                file_bytes = Some(field.bytes().await.unwrap_or_default().to_vec());
            }
            _ => {}
        }
    }

    let bytes = match file_bytes {
        Some(b) if !b.is_empty() => b,
        _ => return (StatusCode::BAD_REQUEST, "missing file").into_response(),
    };

    if file_name.is_empty() { file_name = format!("{}.png", image_id); }

    let path = st.stego_dir.join(&file_name);
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio_fs::create_dir_all(parent).await {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")).into_response();
        }
    }
    if let Err(e) = tokio_fs::write(&path, &bytes).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")).into_response();
    }

    (StatusCode::OK, format!("OK image_id={} sha256={}", image_id, sha)).into_response()
}

/* =========================
   Find stego path for an image_id (search both uploads/ and stego/)
   ========================= */
async fn api_find_stego(
    State(st): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(image_id) = q.get("image_id").cloned() else {
        return (StatusCode::BAD_REQUEST, "missing image_id").into_response();
    };

    async fn scan_dir_for_id(dir: &PathBuf, web_prefix: &str, image_id: &str) -> Option<String> {
        if let Ok(mut rd) = tokio_fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                let lower = name.to_lowercase();
                if lower.contains(&image_id.to_lowercase())
                    && (lower.ends_with(".png") || lower.ends_with(".jpg") || lower.ends_with(".jpeg"))
                {
                    return Some(format!("{}/{}", web_prefix, name));
                }
            }
        }
        None
    }

    if let Some(p) = scan_dir_for_id(&st.stego_dir, "/files/stego", &image_id).await {
        return (StatusCode::OK, Json(FindStegoResp { stego_path: p })).into_response();
    }
    if let Some(p) = scan_dir_for_id(&st.uploads_dir, "/files/uploads", &image_id).await {
        return (StatusCode::OK, Json(FindStegoResp { stego_path: p })).into_response();
    }

    (StatusCode::NOT_FOUND, "not found").into_response()
}

/* =========================
   Client-side Decrypt handler
   ========================= */
async fn api_decrypt(
    mut mp: Multipart,
) -> impl IntoResponse {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut passphrase: Option<String> = None;

    while let Ok(Some(field)) = mp.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
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
    let pass = passphrase.unwrap_or_default();

    let img_rgba = match image::load_from_memory(&bytes) {
        Ok(i) => i.to_rgba8(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode image: {e}")).into_response(),
    };

    let (nonce, ciphertext) = match extract_payload(&img_rgba) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("extract payload: {e}")).into_response(),
    };

    let plaintext = match decrypt_bytes(pass.as_bytes(), &nonce, &ciphertext) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decrypt: {e}")).into_response(),
    };

    let (content_type, filename) = match image::guess_format(&plaintext) {
        Ok(fmt) => {
            use image::ImageFormat::*;
            let (ct, ext) = match fmt {
                Png  => ("image/png",  "png"),
                Jpeg => ("image/jpeg", "jpg"),
                Gif  => ("image/gif",  "gif"),
                Bmp  => ("image/bmp",  "bmp"),
                Tiff => ("image/tiff", "tiff"),
                Ico  => ("image/x-icon","ico"),
                WebP => ("image/webp", "webp"),
                Avif => ("image/avif", "avif"),
                _    => ("application/octet-stream", "bin"),
            };
            (ct.to_string(), format!("decrypted.{}", ext))
        }
        Err(_) => ("application/octet-stream".to_string(), "decrypted.bin".to_string()),
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_str(&content_type).unwrap());
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename)).unwrap(),
    );
    (StatusCode::OK, headers, plaintext).into_response()
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

    // read banner from proxy
    let mut tmp = String::new();
    reader.read_line(&mut tmp).await?;
    tmp.clear();
    reader.read_line(&mut tmp).await?;

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut resp = String::new();
    reader.read_line(&mut resp).await?;
    Ok(resp)
}

async fn read_multiline(proxy_addr: &str, cmd_line: &str) -> anyhow::Result<String> {
    let mut s = TcpStream::connect(proxy_addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // banner
    let mut tmp = String::new();
    reader.read_line(&mut tmp).await?;
    tmp.clear();
    reader.read_line(&mut tmp).await?;

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut out = String::new();
    let mut first = String::new();
    match timeout(Duration::from_secs(1), reader.read_line(&mut first)).await {
        Ok(Ok(n)) if n > 0 => out.push_str(&first),
        _ => anyhow::bail!("empty reply"),
    }
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
   small utils
   ========================= */
fn sanitize(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect()
}

fn now_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[allow(dead_code)]
fn next_op_id() -> String {
    let mut r = [0u8; 4];
    OsRng.fill_bytes(&mut r);
    format!("gui-{}-{:02x}{:02x}{:02x}{:02x}", now_nanos(), r[0], r[1], r[2], r[3])
}