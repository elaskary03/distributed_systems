use axum::{
    extract::{ConnectInfo, Multipart, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use clap::Parser;
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{env, fs, net::SocketAddr, path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    fs as tokio_fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::Mutex,
    time::{sleep, timeout},
};
use tower_http::services::ServeDir;

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
}

#[derive(Clone)]
struct AppState {
    proxy_addr: Arc<String>,
    uploads_dir: Arc<PathBuf>,
    stego_dir: Arc<PathBuf>, // NEW: serve and scan the stego/ folder
    launcher_dir: Arc<PathBuf>,
    launcher: Arc<Mutex<std::collections::HashMap<String, ManagedChild>>>,
}

struct ManagedChild {
    pid: u32,
    port: u16,
    log_path: PathBuf,
    #[allow(dead_code)]
    child: tokio::process::Child,
}

/* =========================
API payloads
========================= */
#[derive(Deserialize)]
struct RegisterReq {
    user: String,
    #[allow(dead_code)]
    password: Option<String>,
    ip: Option<String>, // ignored; kept for backward compatibility
    port: Option<u16>,
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
struct FindStegoResp {
    stego_path: String,
}

#[derive(Serialize)]
struct UploadStegoResp {
    path: String,
}

/* =========================
main
========================= */
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let base_dir = get_data_dir();
    let uploads_dir = base_dir.join("uploads");
    let stego_dir = base_dir.join("stego"); // NEW
    let launcher_dir = base_dir.join("launcher");
    fs::create_dir_all(&uploads_dir).ok();
    fs::create_dir_all(&stego_dir).ok(); // NEW
    fs::create_dir_all(&launcher_dir).ok();

    let state = AppState {
        proxy_addr: Arc::new(args.proxy_addr),
        uploads_dir: Arc::new(uploads_dir.clone()),
        stego_dir: Arc::new(stego_dir.clone()), // NEW
        launcher_dir: Arc::new(launcher_dir.clone()),
        launcher: Arc::new(Mutex::new(std::collections::HashMap::new())),
    };

    // Serve both /files/uploads/* and /files/stego/* (so the browser can download them)
    let files_router = {
        let uploads = uploads_dir.clone();
        let stego = stego_dir.clone();
        Router::new()
            .nest_service(
                "/uploads",
                ServeDir::new(uploads).append_index_html_on_directories(false),
            )
            .nest_service(
                "/stego",
                ServeDir::new(stego).append_index_html_on_directories(false),
            )
    };

    let app = Router::new()
        .route("/", get(ui))
        .route("/api/register", post(api_register))
        .route("/api/unregister", post(api_unregister))
        .route("/api/leader", get(api_leader))
        .route("/api/users", get(api_users))
        .route("/api/peers", get(api_peers))
        .route("/api/list", get(api_list))
        .route("/api/images", get(api_images))
        .route("/api/upload", post(api_upload))
        .route("/api/upload-stego", post(api_upload_stego))
        .route("/api/decrypt", post(api_decrypt)) // client-side decrypt
        .route("/api/find-stego", get(api_find_stego)) // stego discovery for auto-download
        .route("/api/launcher/list", get(api_launcher_list))
        .route("/api/launcher/launch", post(api_launcher_launch))
        .route("/api/launcher/stop", post(api_launcher_stop))
        .nest("/files", files_router)
        .with_state(state);

    let addr: SocketAddr = args.listen.parse()?;
    println!("🖥️  GUI available at http://{}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .await?;
    Ok(())
}

/* =========================
UI (HTML) – light theme, robust layout
========================= */
async fn ui(State(st): State<AppState>) -> impl IntoResponse {
    const PAGE: &str = r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Cloud P2P GUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{
      --bg: #f7f9fc;
      --text: #1f2937;
      --muted: #6b7280;
      --card: #ffffff;
      --border: #e5e7eb;
      --accent: #2563eb;
      --accent-700: #1d4ed8;
      --ok: #16a34a;
      --warn: #b45309;
    }
    *{ box-sizing: border-box; }
    body {
      font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      margin: 0;
    }
    .container{
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px 16px 56px;
    }
    h1{ margin:0 0 6px; font-size: 28px; line-height: 1.2; }
    .sub{ color: var(--muted); margin-bottom: 18px; }

    .grid{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      align-items: start;
    }
    .grid-2-1{
      display: grid;
      gap: 16px;
      grid-template-columns: 2fr 1fr;
    }
    @media (max-width: 960px){
      .grid-2-1{ grid-template-columns: 1fr; }
    }

    section{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      box-shadow: 0 1px 2px rgba(0,0,0,.04);
    }
    section h2{
      margin: 0 0 10px;
      font-size: 18px;
      display:flex; align-items:center; gap:8px;
    }
    .badge{
      font-size: 12px; color: #1e40af; background: #e0e7ff;
      border: 1px solid #c7d2fe; padding: 2px 8px; border-radius: 999px;
    }

    label{ display:block; font-size: 13px; color: var(--muted); margin-bottom: 6px; }
    input[type="text"], input[type="password"], input[type="file"]{
      width: 100%; padding: 10px 12px; border-radius: 10px;
      border: 1px solid var(--border); background: #fff; color: var(--text);
    }
    input:focus{
      outline: none; border-color: #c4d0ff; box-shadow: 0 0 0 3px rgba(37,99,235,.18);
    }
    .row{ display:grid; grid-template-columns: 1fr; gap:12px; }
    @media (min-width: 720px){ .row{ grid-template-columns: 1fr 1fr; } }

    .btns{ display:flex; gap:8px; flex-wrap: wrap; }
    button{
      cursor: pointer; border: 1px solid #d1d5db; background: #f9fafb; color: #111827;
      padding: 8px 12px; border-radius: 10px; font-weight: 600;
    }
    button:hover{ background:#f3f4f6; }
    .btn-accent{ background: var(--accent); border-color: var(--accent); color:#fff; }
    .btn-accent:hover{ background: var(--accent-700); }
    .btn-ok{ color:#fff; background: var(--ok); border-color: var(--ok); }
    .btn-warn{ color:#fff; background: var(--warn); border-color: var(--warn); }

    .out{
      margin-top:10px; background:#f8fafc; border:1px solid var(--border);
      border-radius:10px; padding:10px 12px; min-height: 44px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 13px; white-space: pre-wrap; overflow:auto; max-height: 220px;
    }
    .blurred { filter: blur(4px); pointer-events: none; user-select: none; }
    .user-row{ border:1px solid var(--border); border-radius:10px; padding:10px; margin-bottom:10px; background:#fff; }
    .user-header{ display:flex; align-items:center; gap:8px; font-weight:700; }
    .status-dot{ width:10px; height:10px; border-radius:50%; display:inline-block; }
    .image-list{ margin-top:8px; display:grid; gap:6px; grid-template-columns:1fr; }
    .image-chip{ padding:6px 8px; border:1px dashed var(--border); border-radius:8px; background:#f9fafb; }
    .image-chip code{ background:none; padding:0; }

    .hint{ color: var(--muted); font-size: 12px; margin-top: 8px; }
    .pill{ display:inline-flex; align-items:center; gap:6px; padding:4px 8px; border-radius:999px; font-size:12px; border:1px solid var(--border); background:#fff; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Cloud P2P GUI</h1>
    <div class="sub">Proxy-backed UI for your Raft cluster. Upload an image to encrypt & embed via the cluster. Manage users and inspect state.</div>

    <!-- Login Screen -->
    <section id="login-screen" style="max-width:480px;margin:0 auto 16px; display:block;">
      <h2>🔌 Connect</h2>
      <div class="row">
        <div>
          <label>Username</label>
          <input id="loginUser" type="text" placeholder="alice" required>
        </div>
        <div>
          <label>Password</label>
          <input id="loginPass" type="password" placeholder="optional">
        </div>
      </div>
      <div style="margin-top:10px" class="btns">
        <button id="loginBtn" class="btn-accent">Connect</button>
      </div>
      <div id="loginOut" class="out" style="display:none;"></div>
    </section>

    <!-- Main App (hidden until login) -->
    <div id="main-app" style="display:none;">
      <div style="display:flex; justify-content:flex-end; align-items:center; gap:12px; margin-bottom:8px;">
        <div id="sessionLabel" class="pill">Logged in as: -</div>
        <button id="offlineBtn" class="btn-warn">Go Offline</button>
      </div>

      <!-- Top row: Upload (spans wider) + Leader -->
      <div class="grid-2-1">
        <section>
          <h2>🖼️ Upload & Encrypt <span class="badge">ENCRYPT_ON_CLOUD</span></h2>
          <form id="uploadForm">
            <div class="row">
              <div>
                <label>Image file</label>
                <input type="file" name="file" required>
              </div>
              <div>
                <label>Passphrase</label>
                <input type="password" name="passphrase" placeholder="required by proxy">
              </div>
            </div>
            <div style="margin-top:12px" class="btns">
              <button type="submit" class="btn-accent">Upload & Encrypt</button>
            </div>
          </form>
          <div id="uploadOut" class="out"></div>
          <div class="hint">Upload an image to encrypt and push to the cluster.</div>
        </section>

        <section>
          <h2>👑 Leader</h2>
          <div class="btns">
            <button id="leaderBtn">LEADER</button>
          </div>
          <div id="leaderOut" class="out"></div>
        </section>
      </div>

      <!-- Second row: Inspect (peers only) -->
      <div class="grid">
        <section>
          <h2>📜 Inspect</h2>
          <div class="btns" style="margin-bottom:8px">
            <button id="peersBtn">LIST_PEERS</button>
          </div>
          <div style="margin-top:10px">
            <label>LIST_PEERS Output</label>
            <div id="peersOut" class="out"></div>
          </div>
        </section>
      </div>

      <!-- Third row: P2P Image Sharing -->
      <div class="grid">
        <section>
          <h2>🤝 P2P Image Sharing</h2>
          <div class="row">
            <div>
              <label>Peer Username</label>
              <input id="peerUser" type="text" placeholder="peer-username">
            </div>
            <div>
              <label>Image ID</label>
              <input id="peerImageId" type="text" placeholder="img-...">
            </div>
            <div>
              <label>Requested Views</label>
              <input id="peerViews" type="number" value="1" min="1">
            </div>
            <div>
              <label>Passphrase (for FULL decrypt)</label>
              <input id="peerPassphrase" type="password" placeholder="required to view full image">
            </div>
          </div>
          <div class="btns" style="margin-top:8px">
            <button id="peerListBtn">LIST_IMAGES</button>
            <button id="peerPreviewBtn">PREVIEW</button>
            <button id="peerFullBtn">FULL</button>
            <button id="peerRequestBtn">REQUEST_IMAGE</button>
            <button id="peerRequestMoreBtn">REQUEST_MORE_VIEWS</button>
          </div>
          <div class="hint">FULL will fetch & decrypt for a single in-browser view. Close the viewer to consume one view and refresh quotas. Use REQUEST_IMAGE/REQUEST_MORE_VIEWS to ask for initial or additional view quotas from the owner.</div>
          <div id="peerOut" class="out"></div>
          <div style="margin-top:8px">
            <img id="peerImg" style="max-width:100%; display:none; border:1px solid var(--border); border-radius:8px;" />
          </div>
        </section>
      </div>

      <!-- Pending requests below P2P -->
      <div class="grid">
        <section id="owner-requests" style="display:none;">
          <h2>📬 Pending Requests (Owner)</h2>
          <div class="btns" style="margin-bottom:8px">
            <button id="listRequestsBtn">LIST_REQUESTS</button>
          </div>
          <div id="requestCount" class="hint"></div>
          <div id="requestsList" class="out"></div>
          <div class="row" style="margin-top:10px">
            <div>
              <label>Revoke: Image ID</label>
              <input id="revokeImageId" type="text" placeholder="img-...">
            </div>
            <div>
              <label>Revoke: Target User</label>
              <input id="revokeUser" type="text" placeholder="viewer username">
            </div>
            <div class="btns" style="margin-top:8px">
              <button id="revokeBtn" class="btn-warn">REVOKE ACCESS</button>
            </div>
          </div>
          <div id="revokeOut" class="out"></div>
        </section>
      </div>

    </div>
  </div>

  <!-- Inline viewer for FULL (no download) -->
  <div id="viewerModal" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.65); align-items:center; justify-content:center; padding:20px; z-index:1000;">
    <div id="viewerCard" style="background:#fff; border-radius:12px; max-width:90%; max-height:90%; width:min(960px, 100%); padding:14px; box-shadow:0 20px 60px rgba(0,0,0,.25); display:flex; flex-direction:column; gap:10px;">
      <div style="display:flex; align-items:center; justify-content:space-between; gap:12px;">
        <div id="viewerTitle" style="font-weight:700;">Viewing image</div>
        <button id="viewerClose" class="btn-warn" style="margin:0; padding:6px 10px;">Close</button>
      </div>
      <div id="viewerNotice" class="hint">Close to consume this view; data is not saved to disk.</div>
      <div style="flex:1; overflow:auto; display:flex; justify-content:center; align-items:center; background:#f8fafc; border:1px solid var(--border); border-radius:10px; padding:10px;">
        <img id="viewerImg" style="max-width:100%; max-height:100%; border-radius:8px; display:none;" />
        <div id="viewerText" class="out" style="display:none; width:100%; height:100%; margin:0;"></div>
      </div>
    </div>
  </div>

  <!-- Offline overlay -->
  <div id="offlineOverlay" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.55); backdrop-filter: blur(2px); align-items:center; justify-content:center; z-index:1200; padding:20px;">
    <div style="background:#fff; border-radius:12px; padding:18px; max-width:420px; width:100%; box-shadow:0 20px 50px rgba(0,0,0,.35); display:flex; flex-direction:column; gap:10px; text-align:center;">
      <h3 style="margin:0;">You are offline</h3>
      <p style="margin:0; color:#6b7280;">Presence disconnected. Go online to continue sharing and receiving updates.</p>
      <div class="btns" style="justify-content:center;">
        <button id="goOnlineBtn" class="btn-accent">Go Online</button>
      </div>
    </div>
  </div>

  <script>
    const $ = sel => document.querySelector(sel);
    const text = (id, s) => { const el = $(id); if (el) el.textContent = s; };
    const escapeHtml = (s = '') => s
      .toString()
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
    const WS_URL = "{{WS_URL}}";
    const P2P_BASE = "http://192.168.8.247:10000";
    const DEFAULT_P2P_PORT = 10000;
    const LAUNCH_PORT_DEFAULT = 10002;
    let presenceWs = null;
    let currentUser = "";
    let cachedUsersList = "";
    let manualLogout = false;
    let pendingNewCache = [];
    let pendingMoreCache = [];
    let offlineUser = "";
    let activeViewer = null;
    let lastViewedContext = null;
    const launchStatus = (msg) => { text('#loginOut', msg); document.getElementById('loginOut').style.display = 'block'; };

    function sendOfflineBeacon() {
      if (!currentUser) return;
      try {
        const payload = JSON.stringify({ user: currentUser });
        if (navigator.sendBeacon) {
          navigator.sendBeacon('/api/unregister', new Blob([payload], { type: 'application/json' }));
        } else {
          fetch('/api/unregister', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: payload,
            keepalive: true
          }).catch(() => {});
        }
      } catch (_) {}
    }
    window.addEventListener('beforeunload', sendOfflineBeacon);
    window.addEventListener('pagehide', sendOfflineBeacon);

    async function startLocalClient(user, port = DEFAULT_P2P_PORT) {
      try {
        const res = await fetch('/api/launcher/launch', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user, port })
        });
        const body = await res.text();
        launchStatus(body);
      } catch (err) {
        launchStatus('launcher error: ' + err);
      }
    }

    // Maintain a websocket presence session. Server auto-unregisters on disconnect.
    function connectPresence() {
      if (!currentUser) return;

      if (presenceWs && (presenceWs.readyState === WebSocket.OPEN || presenceWs.readyState === WebSocket.CONNECTING)) {
        return;
      }

      presenceWs = new WebSocket(WS_URL);
      presenceWs.onopen = () => {
        manualLogout = false;
        try { presenceWs.send(`REGISTER ${currentUser} ${DEFAULT_P2P_PORT}`); } catch (e) { console.error(e); }
      };
      presenceWs.onmessage = (evt) => {
        if (typeof evt.data === 'string') {
          cachedUsersList = evt.data;
        }
      };
      presenceWs.onclose = () => {
        presenceWs = null;
        if (manualLogout) { manualLogout = false; return; }
        cachedUsersList = "";
        clearOutputs();
        currentUser = "";
        showMainUI(false);
        document.getElementById('sessionLabel').textContent = 'Logged in as: -';
        text('#loginOut', 'Connection closed. Please log in again.');
        document.getElementById('loginOut').style.display = 'block';
      };
      presenceWs.onerror = (e) => {
        console.error('ws error', e);
        try { presenceWs.close(); } catch (_) {}
      };
    }

    function closeViewer(refresh = true) {
      const modal = $('#viewerModal');
      if (!modal) return;
      const img = $('#viewerImg');
      const txt = $('#viewerText');
      if (activeViewer?.url) { URL.revokeObjectURL(activeViewer.url); }
      if (img) { img.src = ''; img.style.display = 'none'; }
      if (txt) { txt.textContent = ''; txt.style.display = 'none'; }
      modal.style.display = 'none';
      activeViewer = null;
      if (refresh && lastViewedContext) {
        refreshPeerList().catch(() => {});
      }
      lastViewedContext = null;
    }

    function openViewer(blob, opts = {}) {
      const modal = $('#viewerModal');
      const img = $('#viewerImg');
      const txt = $('#viewerText');
      if (!modal || !img || !txt) return;
      closeViewer(false);
      const url = URL.createObjectURL(blob);
      const ct = opts.contentType || '';
      const isImage = ct.startsWith('image/');
      if (isImage) {
        img.src = url;
        img.style.display = 'block';
        txt.style.display = 'none';
      } else {
        img.style.display = 'none';
        txt.style.display = 'block';
        txt.textContent = opts.textContent || `Binary payload (${ct || 'unknown'})`;
      }
      document.getElementById('viewerTitle').textContent = opts.title || 'Viewing image';
      document.getElementById('viewerNotice').textContent = opts.notice || 'Close to consume this view.';
      modal.style.display = 'flex';
      activeViewer = { url };
      lastViewedContext = opts.context || null;
    }

    document.getElementById('viewerClose').addEventListener('click', () => closeViewer(true));
    document.getElementById('viewerModal').addEventListener('click', (e) => {
      if (e.target && e.target.id === 'viewerModal') {
        closeViewer(true);
      }
    });

    function clearOutputs() {
      ['uploadOut','peersOut','leaderOut','loginOut','peerOut'].forEach(id => text('#'+id, ''));
      const img = $('#peerImg'); if (img) { img.style.display = 'none'; img.src = ''; }
      const overlay = document.getElementById('offlineOverlay'); if (overlay) overlay.style.display = 'none';
      const main = document.getElementById('main-app'); if (main) main.classList.remove('blurred');
      closeViewer(false);
    }

    function showMainUI(show) {
      const login = document.getElementById('login-screen');
      const main = document.getElementById('main-app');
      if (show) {
        login.style.display = 'none';
        main.style.display = 'block';
      } else {
        login.style.display = 'block';
        main.style.display = 'none';
      }
    }

    function renderUserImages(users = []) {
      if (!users.length) return 'No users found';
      return users.map(u => {
        const name = escapeHtml(u.user || u.username || 'unknown');
        const imgs = Array.isArray(u.images) ? u.images : [];
        const color = u.online ? '#16a34a' : '#dc2626';
        const status = u.online ? 'online' : 'offline';
        const lastSeen = formatLastSeen(u.last_seen);
        const imagesHtml = imgs.length
          ? imgs.map(img => {
              const id = escapeHtml(img.id || 'unknown');
              const owner = escapeHtml(img.owner || '');
              const remaining = img.remaining_views_for_requester !== undefined
                ? `Remaining (you): ${img.remaining_views_for_requester}`
                : 'Remaining: -';
              const sharedPw = img.shared_passphrase
                ? `Passphrase: ${escapeHtml(img.shared_passphrase)}`
                : '';
              const perms = img.permissions && typeof img.permissions === 'object'
                ? `Permissions: ${Object.entries(img.permissions).map(([k,v]) => `${escapeHtml(k)}=${v}`).join(', ')}`
                : '';
              return `
                <div class="image-chip" style="display:flex; flex-direction:column; align-items:flex-start; gap:4px;">
                  <div><code>${id}</code>${owner ? ` • owner: ${owner}` : ''}</div>
                  <div class="hint">${remaining}</div>
                  ${sharedPw ? `<div class="hint">${sharedPw}</div>` : ''}
                  ${perms ? `<div class="hint">${perms}</div>` : ''}
                </div>
              `;
            }).join('')
          : '<div class="image-chip">No images</div>';
        return `
<div class="user-row" style="padding:8px 10px; border:1px solid var(--border); border-radius:10px; margin-bottom:10px; background:#f9fafb;">
  <div class="user-header" style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
    <span class="status-dot" style="background:${color}"></span>
    <strong>${name}</strong>
    <span class="pill">${status}</span>
    <span class="hint">IP: ${escapeHtml(u.ip || '-')}</span>
    <span class="hint">Last seen: ${lastSeen}</span>
  </div>
  <div class="image-list" style="margin-top:8px; display:flex; flex-direction:column; gap:8px;">
    ${imagesHtml}
  </div>
</div>
        `.trim();
      }).join('\n');
    }

    // Ask the server to locate the stego file path for this image_id
    async function pollFindStego(imageId, timeoutMs = 15000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const res = await fetch('/api/find-stego?image_id=' + encodeURIComponent(imageId));
        if (res.ok) {
          const json = await res.json();
          return json.stego_path; // /files/uploads/<..> or /files/stego/<..>
        }
        await new Promise(r => setTimeout(r, 600));
      }
      return null;
    }

    /* Login */
    $('#loginBtn').addEventListener('click', async () => {
      const user = ($('#loginUser').value || '').trim();
      const pass = ($('#loginPass').value || '').trim();
      if (!user) {
        text('#loginOut', 'Username is required');
        document.getElementById('loginOut').style.display = 'block';
        return;
      }
      currentUser = user;
      document.getElementById('sessionLabel').textContent = `Logged in as: ${user} (IP auto)`;
      showMainUI(true);
      document.getElementById('loginOut').style.display = 'none';
      clearOutputs();
      connectPresence();
      startLocalClient(user).catch(() => {});
      // Pull initial user list
      try {
        const list = await (await fetch('/api/users')).text();
        cachedUsersList = list;
      } catch (_) {}
      await refreshOwnerRequests();
    });

    /* Upload & ENCRYPT_ON_CLOUD */
    $('#uploadForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: fd });
        if (!res.ok) { text('#uploadOut', await res.text()); return; }
        const json = await res.json();

        // Try to find the stego file and download it
        const imageId = json.image_id;
        const stegoPath = await pollFindStego(imageId);
        if (stegoPath) {
          const fileBase = stegoPath.split('/').pop() || `stego-${imageId}.png`;
          try {
            // fetch the stego PNG
            const pngResp = await fetch(stegoPath);
            const pngBlob = await pngResp.blob();
            // fetch original for preview
            let previewBlob = null;
            if (json.original_path) {
              try {
                const origResp = await fetch(json.original_path);
                previewBlob = await origResp.blob();
              } catch (_) {}
            }
            const fdUpload = new FormData();
            fdUpload.append('image_id', imageId);
            fdUpload.append('owner', currentUser);
            fdUpload.append('permissions', JSON.stringify({}));
            fdUpload.append('file', new File([pngBlob], fileBase, { type: 'image/png' }));
            if (previewBlob) {
              fdUpload.append('preview', new File([previewBlob], `preview-${fileBase}`, { type: 'image/png' }));
            }

            let uploadTarget = null;
            try { uploadTarget = await resolvePeer(currentUser); } catch (_) {}
            const p2pUrl = `${P2P_BASE}/upload-image`;
            await fetch(p2pUrl, { method: 'POST', body: fdUpload });

            // Download locally for user convenience
            const a = document.createElement('a');
            a.href = stegoPath;
            a.download = fileBase;
            a.click();
            text('#uploadOut', '✅ Done');
          } catch (err) {
            text('#uploadOut', `⚠️ Uploaded but P2P push failed: ${err}`);
          }
        } else {
          text('#uploadOut', '⚠️ Uploaded; awaiting stego generation.');
        }
      } catch (err) { text('#uploadOut', String(err)); }
    });

    /* LIST_PEERS */
    $('#peersBtn').addEventListener('click', async () => {
      try {
        const raw = await ensureUsersList();
        const users = parseUsers(raw);
        if (!users.length) { text('#peersOut', 'No peers found'); return; }
        const rows = users.map(u => {
          const status = u.online ? 'online' : 'offline';
          const dot = u.online ? '🟢' : '🔴';
          return [
            `name: ${u.user}`,
            `ip: ${u.ip || '-'}`,
            `status: ${dot} ${status}`,
            `last seen: ${formatLastSeen(u.last_seen)}`
          ].join('\n');
        });
        text('#peersOut', rows.join('\n\n'));
      }
      catch (err) { text('#peersOut', String(err)); }
    });

    /* LEADER */
    $('#leaderBtn').addEventListener('click', async () => {
      try { text('#leaderOut', await (await fetch('/api/leader')).text()); }
      catch (err) { text('#leaderOut', String(err)); }
    });

    async function ensureUsersList() {
      if (!cachedUsersList) {
        const resp = await fetch('/api/users');
        cachedUsersList = await resp.text();
      }
      return cachedUsersList;
    }

    function parseUsers(body) {
      const lines = body.split('\n').map(l => l.trim()).filter(Boolean);
      return lines
        .map(l => {
          const parts = l.split(/\s+/);
          if (parts.length < 4) return null;
          const [user, ip, port, online, lastSeenRaw] = [parts[0], parts[1], parts[2], parts[3], parts[4]];
          return {
            user,
            ip,
            port: parseInt(port, 10) || DEFAULT_P2P_PORT,
            online: online === 'true',
            last_seen: lastSeenRaw ? parseInt(lastSeenRaw, 10) || 0 : 0,
          };
        })
        .filter(Boolean);
    }

    async function getOnlinePeers() {
      const body = await ensureUsersList();
      return parseUsers(body).filter(u => u.online && !!u.ip);
    }

    function formatLastSeen(ns = 0) {
      if (!ns) return 'unknown';
      const ms = ns / 1_000_000;
      const diffMs = Date.now() - ms;
      if (diffMs < 0) return 'just now';
      const mins = Math.floor(diffMs / 60000);
      if (mins < 1) return 'just now';
      if (mins < 60) return `${mins}m ago`;
      const hours = Math.floor(mins / 60);
      if (hours < 24) return `${hours}h ago`;
      const days = Math.floor(hours / 24);
      return `${days}d ago`;
    }

    async function resolvePeer(name, opts = {}) {
      const peers = await getOnlinePeers();
      const found = peers.find(p => p.user === name);
      if (found) return found;
      if (opts.allowOffline) {
        const all = parseUsers(await ensureUsersList());
        return all.find(p => p.user === name && !!p.ip) || null;
      }
      return null;
    }

    async function fetchCachedImages() {
      try {
        const res = await fetch('/api/images');
        if (!res.ok) return [];
        const data = await res.json();
        let users = Array.isArray(data.users) ? data.users : [];
        if (!users.length) {
          // Fallback: build minimal entries from SHOW_USERS so offline users still render
          try {
            const raw = await (await fetch('/api/users')).text();
            users = parseUsers(raw).map(u => ({
              user: u.user,
              username: u.user,
              online: u.online,
              ip: u.ip,
              p2p_port: u.port,
              last_seen: u.last_seen || 0,
              images: [],
            }));
          } catch (_) {}
        }
        return users;
      } catch (_) { return []; }
    }

    async function fetchLiveImages() {
      const peers = await getOnlinePeers();
      const results = [];
      for (const peer of peers) {
        const owner = peer.user;
        if (!owner) continue;
        const url = `${P2P_BASE}/list-images?owner=${encodeURIComponent(owner)}&requester=${encodeURIComponent(currentUser || '')}`;
        try {
          const res = await fetch(url);
          const json = await res.json();
          const images = (json.images || []).filter(img => !img.owner || img.owner === owner);
          results.push({
            user: owner,
            username: owner,
            online: true,
            ip: peer.ip,
            p2p_port: peer.port,
            last_seen: peer.last_seen || 0,
            status: json.status || 'ok',
            images,
          });
        } catch (err) {
          results.push({ user: peer.user, username: peer.user, online: true, error: String(err), images: [] });
        }
      }
      return results;
    }

    function mergeUsers(cached, live) {
      const map = new Map();
      for (const u of cached || []) {
        const key = u.user || u.username || '';
        if (!key) continue;
        map.set(key, { ...u, user: key, username: key });
      }
      for (const u of live || []) {
        const key = u.user || u.username || '';
        if (!key) continue;
        const existing = map.get(key) || {};
        map.set(key, {
          ...existing,
          ...u,
          user: key,
          username: key,
          images: u.images || existing.images || [],
          online: u.online !== undefined ? u.online : existing.online,
          ip: u.ip || existing.ip || '',
          p2p_port: u.p2p_port || existing.p2p_port || DEFAULT_P2P_PORT,
          last_seen: u.last_seen || existing.last_seen || 0,
        });
      }
      return Array.from(map.values());
    }

    async function fetchBinary(url) {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`status ${res.status}`);
      const ct = res.headers.get('content-type') || '';
      const buf = await res.arrayBuffer();
      return { ct, buf };
    }

    async function refreshOwnerRequests() {
      if (!currentUser) { return; }
      try {
        const url = `${P2P_BASE}/list-images?owner=${encodeURIComponent(currentUser)}&requester=${encodeURIComponent(currentUser)}`;
        const res = await fetch(url);
        if (!res.ok) return;
        const data = await res.json();
        const pendingNew = [];
        const pendingMore = [];
        for (const img of data.images || []) {
          if (img.owner === currentUser && Array.isArray(img.pending_requests)) {
            const perms = (img.permissions && typeof img.permissions === 'object') ? img.permissions : {};
            for (const req of img.pending_requests) {
              const existing = typeof perms[req.viewer] === 'number' ? perms[req.viewer] : 0;
              const bucket = existing > 0 ? pendingMore : pendingNew;
              bucket.push({ image: img.id, viewer: req.viewer, requested: req.requested_views, existing });
            }
          }
        }
        pendingNewCache = pendingNew;
        pendingMoreCache = pendingMore;
        const total = pendingNew.length + pendingMore.length;
        text('#requestCount', `${total} pending request(s): ${pendingNew.length} new, ${pendingMore.length} more-views`);
        renderRequests();
        document.getElementById('owner-requests').style.display = 'block';
      } catch (_) {}
    }

    async function revokeAccess() {
      const imageId = ($('#revokeImageId').value || '').trim();
      const target = ($('#revokeUser').value || '').trim();
      if (!currentUser) { text('#revokeOut', 'Login first'); return; }
      if (!imageId || !target) { text('#revokeOut', 'Image ID and target user required'); return; }
      try {
        const url = `${P2P_BASE}/revoke-access/${encodeURIComponent(currentUser)}/${encodeURIComponent(imageId)}`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ target_user: target }),
        });
        if (!res.ok) {
          text('#revokeOut', `Error: ${await res.text()}`);
          return;
        }
        const data = await res.json();
        text('#revokeOut', JSON.stringify(data, null, 2));
        await refreshOwnerRequests();
      } catch (err) {
        text('#revokeOut', String(err));
      }
    }

    function renderRequests() {
      const container = document.getElementById('requestsList');
      const renderGroup = (list, kind) => {
        if (!list.length) return '<div class="hint">None</div>';
        return list.map((req, idx) => {
          return `
          <div class="req" data-img="${req.image}" data-viewer="${req.viewer}" data-kind="${kind}" data-idx="${idx}">
            <div><strong>Image:</strong> ${req.image}</div>
            <div><strong>Viewer:</strong> ${req.viewer}</div>
            <div><strong>Requested:</strong> ${req.requested}${req.existing ? ` (already had ${req.existing})` : ''}</div>
            <div style="margin-top:6px; display:flex; gap:8px; align-items:center;">
              <input type="number" id="${kind}-approve-${idx}" value="${req.requested}" min="1" style="width:90px;">
              <input type="text" id="${kind}-pass-${idx}" placeholder="passphrase (optional)" style="width:180px;">
              <button class="approve-btn" data-kind="${kind}" data-idx="${idx}">Approve</button>
              <button class="reject-btn" data-kind="${kind}" data-idx="${idx}">Reject</button>
            </div>
          </div>`;
        }).join('\n');
      };

      const any = pendingNewCache.length + pendingMoreCache.length;
      if (!any) {
        container.textContent = 'No pending requests';
        return;
      }
      container.innerHTML = `
        <div style="margin-bottom:10px;">
          <h4>New access requests</h4>
          ${renderGroup(pendingNewCache, 'new')}
        </div>
        <div>
          <h4>More views requests</h4>
          ${renderGroup(pendingMoreCache, 'more')}
        </div>
      `;
    }

    /* P2P actions */
    async function refreshPeerList() {
      try {
        text('#peerOut', 'Loading image metadata...');
        const [cached, live] = await Promise.all([
          fetchCachedImages(),
          fetchLiveImages().catch(() => []),
        ]);
        const merged = mergeUsers(cached, live);
        if (!merged.length) { text('#peerOut', 'no users found'); return; }
        const out = document.getElementById('peerOut');
        out.innerHTML = renderUserImages(merged);
      } catch (err) { text('#peerOut', String(err)); }
    }

    $('#peerListBtn').addEventListener('click', refreshPeerList);

    $('#peerPreviewBtn').addEventListener('click', async () => {
      const peer = ($('#peerUser').value || '').trim();
      const imgId = ($('#peerImageId').value || '').trim();
      if (!peer || !imgId) { text('#peerOut', 'peer username and image_id required'); return; }
      try {
        const info = await resolvePeer(peer);
        if (!info) { text('#peerOut', `peer ${peer} not found/online`); return; }
        const owner = info.user || peer;
        const url = `${P2P_BASE}/preview/${encodeURIComponent(owner)}/${encodeURIComponent(imgId)}`;
        const { ct, buf } = await fetchBinary(url);
        if (ct.startsWith('image/')) {
          const blob = new Blob([buf], { type: ct });
          const obj = URL.createObjectURL(blob);
          const img = $('#peerImg'); img.style.display = 'block'; img.src = obj;
          text('#peerOut', `Preview from ${peer}/${imgId}`);
        } else {
          text('#peerOut', `Preview response: ${ct}`);
        }
      } catch (err) { text('#peerOut', String(err)); }
    });

    $('#peerFullBtn').addEventListener('click', async () => {
      const peer = ($('#peerUser').value || '').trim();
      const imgId = ($('#peerImageId').value || '').trim();
      const pass = ($('#peerPassphrase').value || '').trim();
      if (!currentUser) { text('#peerOut', 'login required to view full images'); return; }
      if (!peer || !imgId) { text('#peerOut', 'peer username and image_id required'); return; }
      if (!pass) { text('#peerOut', 'passphrase required for FULL view'); return; }
      try {
        const info = await resolvePeer(peer);
        if (!info) { text('#peerOut', `peer ${peer} not found/online`); return; }
        const owner = info.user || peer;
        const url = `${P2P_BASE}/full/${encodeURIComponent(owner)}/${encodeURIComponent(imgId)}?requester=${encodeURIComponent(currentUser)}`;
        const res = await fetch(url);
        const ct = res.headers.get('content-type') || '';
        if (ct.startsWith('image/')) {
          const stegoBlob = await res.blob();
          const fd = new FormData();
          fd.append('file', stegoBlob, `${imgId}.png`);
          fd.append('passphrase', pass);
          const decRes = await fetch('/api/decrypt', { method: 'POST', body: fd });
          if (!decRes.ok) {
            text('#peerOut', await decRes.text());
            return;
          }
          const decCt = decRes.headers.get('content-type') || '';
          const decBlob = await decRes.blob();
          // Mark the view as consumed only after successful decrypt
          try {
            const consumeUrl = `${P2P_BASE}/consume-view/${encodeURIComponent(owner)}/${encodeURIComponent(imgId)}`;
            await fetch(consumeUrl, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ requester: currentUser })
            });
          } catch (_) {}
          let textPayload = null;
          if (!decCt.startsWith('image/')) {
            try { textPayload = await decBlob.text(); } catch (_) {}
          }
          openViewer(decBlob, {
            contentType: decCt,
            textContent: textPayload,
            title: `Viewing ${peer}/${imgId}`,
            notice: 'Close the viewer to refresh remaining views; nothing is saved to disk.',
            context: { peer, imgId }
          });
          text('#peerOut', `Viewing decrypted image from ${peer}/${imgId}. Close the viewer when done.`);
        } else {
          const textResp = await res.text();
          text('#peerOut', textResp);
        }
      } catch (err) { text('#peerOut', String(err)); }
    });

    $('#listRequestsBtn').addEventListener('click', async () => {
      await refreshOwnerRequests();
    });

    $('#revokeBtn').addEventListener('click', async () => {
      await revokeAccess();
    });

    document.getElementById('requestsList').addEventListener('click', async (e) => {
      const btn = e.target;
      if (btn.classList.contains('approve-btn')) {
        const idx = parseInt(btn.dataset.idx || '-1', 10);
        const kind = btn.dataset.kind || 'new';
        const list = kind === 'more' ? pendingMoreCache : pendingNewCache;
        const req = list[idx];
        if (!req) return;
        const input = document.getElementById(`${kind}-approve-${idx}`);
        const approved = parseInt((input?.value || '0'), 10);
        if (!approved || approved <= 0) { text('#requestsList', 'Approved views must be positive'); return; }
        const passInput = document.getElementById(`${kind}-pass-${idx}`);
        const passphrase = (passInput?.value || '').trim();
        const url = `${P2P_BASE}/approve-request/${encodeURIComponent(currentUser)}/${encodeURIComponent(req.image)}`;
        await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ requester: req.viewer, views: approved, passphrase }),
        });
        await refreshOwnerRequests();
      } else if (btn.classList.contains('reject-btn')) {
        const idx = parseInt(btn.dataset.idx || '-1', 10);
        const kind = btn.dataset.kind || 'new';
        const list = kind === 'more' ? pendingMoreCache : pendingNewCache;
        const req = list[idx];
        if (!req) return;
        const url = `${P2P_BASE}/reject-request/${encodeURIComponent(currentUser)}/${encodeURIComponent(req.image)}`;
        await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ requester: req.viewer, views: 0 }),
        });
        await refreshOwnerRequests();
      }
    });

    $('#peerRequestBtn').addEventListener('click', async () => {
      const peer = ($('#peerUser').value || '').trim();
      const imgId = ($('#peerImageId').value || '').trim();
      const views = parseInt($('#peerViews').value || '0', 10);
      if (!peer || !imgId || !currentUser) { text('#peerOut', 'peer, image_id, and login required'); return; }
      if (views <= 0) { text('#peerOut', 'views must be positive'); return; }
      try {
        // For requests we allow targeting offline peers using last-known IP/port
        const info = await resolvePeer(peer, { allowOffline: true });
        if (!info) { text('#peerOut', `peer ${peer} not found/online`); return; }
        const owner = info.user || peer;
        const url = `${P2P_BASE}/request-image/${encodeURIComponent(owner)}/${encodeURIComponent(imgId)}`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ requester: currentUser, views }),
        });
        const txt = await res.text();
        text('#peerOut', txt);
        await refreshOwnerRequests();
      } catch (err) { text('#peerOut', String(err)); }
    });

    $('#peerRequestMoreBtn').addEventListener('click', async () => {
      const peer = ($('#peerUser').value || '').trim();
      const imgId = ($('#peerImageId').value || '').trim();
      const views = parseInt($('#peerViews').value || '0', 10);
      if (!peer || !imgId || !currentUser) { text('#peerOut', 'peer, image_id, and login required'); return; }
      if (views <= 0) { text('#peerOut', 'views must be positive'); return; }
      try {
        const info = await resolvePeer(peer, { allowOffline: true });
        if (!info) { text('#peerOut', `peer ${peer} not found/online`); return; }
        const owner = info.user || peer;
        const url = `${P2P_BASE}/request-image/${encodeURIComponent(owner)}/${encodeURIComponent(imgId)}`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ requester: currentUser, views }),
        });
        const txt = await res.text();
        text('#peerOut', txt);
        await refreshOwnerRequests();
      } catch (err) { text('#peerOut', String(err)); }
    });

    /* Local launcher */
    $('#launchBtn').addEventListener('click', async () => {
      const user = ($('#launchUser').value || '').trim();
      const port = parseInt($('#launchPort').value || `${LAUNCH_PORT_DEFAULT}`, 10);
      if (!user || port <= 0) { text('#launchOut', 'username and valid port required'); return; }
      try {
        const res = await fetch('/api/launcher/launch', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user, port })
        });
        const body = await res.text();
        text('#launchOut', body);
      } catch (err) { text('#launchOut', String(err)); }
    });

    $('#stopBtn').addEventListener('click', async () => {
      const user = ($('#launchUser').value || '').trim();
      if (!user) { text('#launchOut', 'username required to stop'); return; }
      try {
        const res = await fetch('/api/launcher/stop', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user })
        });
        const body = await res.text();
        text('#launchOut', body);
      } catch (err) { text('#launchOut', String(err)); }
    });

    $('#listLaunchBtn').addEventListener('click', async () => {
      try {
        const res = await fetch('/api/launcher/list');
        const body = await res.text();
        text('#launchOut', body);
      } catch (err) { text('#launchOut', String(err)); }
    });

    /* Go Offline */
    $('#offlineBtn').addEventListener('click', async () => {
      if (!currentUser) { return; }
      try {
        await fetch('/api/unregister', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user: currentUser })
        });
      } catch (_) {}
      offlineUser = currentUser;
      cachedUsersList = "";
      manualLogout = true;
      if (presenceWs) { try { presenceWs.close(); } catch (_) {} presenceWs = null; }
      const main = document.getElementById('main-app');
      if (main) { main.classList.add('blurred'); }
      const overlay = document.getElementById('offlineOverlay');
      if (overlay) { overlay.style.display = 'flex'; }
      document.getElementById('sessionLabel').textContent = `Logged in as: ${offlineUser} (offline)`;
    });

    $('#goOnlineBtn').addEventListener('click', async () => {
      const user = offlineUser || currentUser;
      if (!user) { showMainUI(false); return; }
      currentUser = user;
      cachedUsersList = "";
      manualLogout = false;
      const main = document.getElementById('main-app');
      if (main) { main.classList.remove('blurred'); }
      const overlay = document.getElementById('offlineOverlay');
      if (overlay) { overlay.style.display = 'none'; }
      document.getElementById('sessionLabel').textContent = `Logged in as: ${currentUser} (online)`;
      offlineUser = "";
      connectPresence();
      await refreshOwnerRequests();
      await refreshPeerList();
    });

  </script>
</body>
</html>
"#;

    let ws_url = format!("ws://{}", st.proxy_addr);
    let page = PAGE.replace("{{WS_URL}}", &ws_url);

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "text/html; charset=utf-8".parse().unwrap(),
    );
    (StatusCode::OK, headers, Html(page))
}

/* =========================
API handlers
========================= */

async fn api_register(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterReq>,
) -> impl IntoResponse {
    if payload.user.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "user is required").into_response();
    }
    let ip = payload
        .ip
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| peer.ip().to_string());
    let port = payload.port.unwrap_or(10000);
    let line = format!("REGISTER {} {} {}", payload.user, ip, port);
    proxy_send_oneline(&st.proxy_addr, &line)
        .await
        .into_response()
}

async fn api_unregister(
    State(st): State<AppState>,
    Json(payload): Json<UnregisterReq>,
) -> impl IntoResponse {
    if payload.user.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "user is required").into_response();
    }
    let line = format!("UNREGISTER {}", payload.user);
    proxy_send_oneline(&st.proxy_addr, &line)
        .await
        .into_response()
}

async fn api_leader(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "LEADER")
        .await
        .into_response()
}
async fn api_users(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "SHOW_USERS")
        .await
        .into_response()
}
async fn api_peers(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "LIST_PEERS")
        .await
        .into_response()
}
async fn api_list(State(st): State<AppState>) -> impl IntoResponse {
    proxy_send_multiline(&st.proxy_addr, "LIST")
        .await
        .into_response()
}
async fn api_images(State(st): State<AppState>) -> impl IntoResponse {
    let (status, body) = proxy_send_multiline(&st.proxy_addr, "IMAGE_METADATA").await;
    if status != StatusCode::OK {
        return (status, body).into_response();
    }
    match serde_json::from_str::<Value>(&body) {
        Ok(json) => (StatusCode::OK, Json(json)).into_response(),
        Err(_) => (StatusCode::BAD_GATEWAY, body).into_response(),
    }
}

/* =========================
Upload handler (encrypt-on-cloud trigger)
========================= */
async fn api_upload(State(st): State<AppState>, mut mp: Multipart) -> impl IntoResponse {
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

    let image_id = format!("img-{}", now_nanos());
    let original_path = st
        .uploads_dir
        .join(format!("{}-{}", image_id, sanitize(&filename)));

    if let Err(e) = tokio_fs::write(&original_path, &bytes).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("write original: {e}"),
        )
            .into_response();
    }

    // Ask the proxy to coordinate the cluster-side encryption/stego
    let pass = passphrase.unwrap_or_default();
    let cmd = format!("ENCRYPT_ON_CLOUD {} {}", image_id, pass);
    let (status, resp) = proxy_send_oneline(&st.proxy_addr, &cmd).await;

    let resp = UploadResp {
        image_id,
        original_path: format!(
            "/files/uploads/{}",
            original_path.file_name().unwrap().to_string_lossy()
        ),
        status: resp,
    };

    (status, Json(resp)).into_response()
}

/* =========================
Accept stego uploads from nodes (HTTP POST body)
========================= */
async fn api_upload_stego(
    State(st): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: Bytes,
) -> impl IntoResponse {
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty body").into_response();
    }

    let image_id = q
        .get("image_id")
        .cloned()
        .unwrap_or_else(|| format!("stego-{}", now_nanos()));
    let filename = q
        .get("filename")
        .cloned()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("{}.png", image_id));
    let sanitized = sanitize(&filename);

    let path = st.stego_dir.join(&sanitized);
    if let Some(parent) = path.parent() {
        let _ = tokio_fs::create_dir_all(parent).await;
    }

    if let Err(e) = tokio_fs::write(&path, &body).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("write stego: {e}"),
        )
            .into_response();
    }

    let served = format!(
        "/files/stego/{}",
        path.file_name().unwrap().to_string_lossy()
    );
    (StatusCode::OK, Json(UploadStegoResp { path: served })).into_response()
}

/* =========================
Find stego path for an image_id (stego/, optional uploads/ fallback)
========================= */
async fn api_find_stego(
    State(st): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(image_id) = q.get("image_id").cloned() else {
        return (StatusCode::BAD_REQUEST, "missing image_id").into_response();
    };
    let include_uploads = q
        .get("include_uploads")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Helper to scan a directory and return a served path prefix
    async fn scan_dir_for_id(dir: &PathBuf, web_prefix: &str, image_id: &str) -> Option<String> {
        if let Ok(mut rd) = tokio_fs::read_dir(dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                let lower = name.to_lowercase();
                if lower.contains(&image_id.to_lowercase())
                    && (lower.ends_with(".png")
                        || lower.ends_with(".jpg")
                        || lower.ends_with(".jpeg"))
                {
                    return Some(format!("{}/{}", web_prefix, name));
                }
            }
        }
        None
    }

    // Prefer stego dir; only fall back to uploads when explicitly requested.
    if let Some(p) = scan_dir_for_id(&st.stego_dir, "/files/stego", &image_id).await {
        return (StatusCode::OK, Json(FindStegoResp { stego_path: p })).into_response();
    }
    if include_uploads {
        if let Some(p) = scan_dir_for_id(&st.uploads_dir, "/files/uploads", &image_id).await {
            return (StatusCode::OK, Json(FindStegoResp { stego_path: p })).into_response();
        }
    }

    (StatusCode::NOT_FOUND, "not found").into_response()
}

/* =========================
Client-side Decrypt handler
========================= */
async fn api_decrypt(mut mp: Multipart) -> impl IntoResponse {
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

    // Decode the uploaded stego image to RGBA8
    let img_rgba = match image::load_from_memory(&bytes) {
        Ok(i) => i.to_rgba8(),
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode image: {e}")).into_response(),
    };

    // Extract (nonce, ciphertext) from LSBs and decrypt
    let (nonce, ciphertext) = match extract_payload(&img_rgba) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("extract payload: {e}")).into_response()
        }
    };

    let plaintext = match decrypt_bytes(pass.as_bytes(), &nonce, &ciphertext) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decrypt: {e}")).into_response(),
    };

    // Try to detect if plaintext is an image; set correct headers and filename
    let (content_type, filename) = match image::guess_format(&plaintext) {
        Ok(fmt) => {
            use image::ImageFormat::*;
            let (ct, ext) = match fmt {
                Png => ("image/png", "png"),
                Jpeg => ("image/jpeg", "jpg"),
                Gif => ("image/gif", "gif"),
                Bmp => ("image/bmp", "bmp"),
                Tiff => ("image/tiff", "tiff"),
                Ico => ("image/x-icon", "ico"),
                WebP => ("image/webp", "webp"),
                Avif => ("image/avif", "avif"),
                _ => ("application/octet-stream", "bin"),
            };
            (ct.to_string(), format!("decrypted.{}", ext))
        }
        Err(_) => (
            "application/octet-stream".to_string(),
            "decrypted.bin".to_string(),
        ),
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type).unwrap(),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("inline; filename=\"{}\"", filename)).unwrap(),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    (StatusCode::OK, headers, plaintext).into_response()
}

/* =========================
Local client launcher
========================= */
#[derive(Deserialize)]
struct LaunchReq {
    user: String,
    port: u16,
}

#[derive(Deserialize)]
struct StopReq {
    user: String,
}

async fn api_launcher_list(State(st): State<AppState>) -> impl IntoResponse {
    let map = st.launcher.lock().await;
    let mut entries: Vec<String> = map
        .iter()
        .map(|(u, child)| {
            format!(
                "{}: pid={} port={} log={}",
                u,
                child.pid,
                child.port,
                child.log_path.display()
            )
        })
        .collect();
    entries.sort();
    let body = if entries.is_empty() {
        "no local clients".to_string()
    } else {
        entries.join("\n")
    };
    (StatusCode::OK, body).into_response()
}

async fn api_launcher_launch(
    State(st): State<AppState>,
    Json(payload): Json<LaunchReq>,
) -> impl IntoResponse {
    let user = payload.user.trim();
    if user.is_empty() || payload.port == 0 {
        return (StatusCode::BAD_REQUEST, "user and valid port required").into_response();
    }
    {
        let map = st.launcher.lock().await;
        if map.contains_key(user) {
            return (StatusCode::BAD_REQUEST, "already running").into_response();
        }
    }

    let log_path = st.launcher_dir.join(format!("{}.log", user));
    let log_file = match std::fs::File::create(&log_path) {
        Ok(f) => f,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("log create: {e}"),
            )
                .into_response()
        }
    };
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--bin")
        .arg("client_p2p")
        .arg("--")
        .arg("--user")
        .arg(user)
        .arg("--port")
        .arg(payload.port.to_string())
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file));

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")).into_response()
        }
    };
    let pid = child.id().unwrap_or(0);

    st.launcher.lock().await.insert(
        user.to_string(),
        ManagedChild {
            pid,
            port: payload.port,
            log_path: log_path.clone(),
            child,
        },
    );

    (
        StatusCode::OK,
        format!(
            "launched {} on port {} (pid {}) log={}",
            user,
            payload.port,
            pid,
            log_path.display()
        ),
    )
        .into_response()
}

async fn api_launcher_stop(
    State(st): State<AppState>,
    Json(payload): Json<StopReq>,
) -> impl IntoResponse {
    let user = payload.user.trim();
    if user.is_empty() {
        return (StatusCode::BAD_REQUEST, "user required").into_response();
    }
    let mut guard = st.launcher.lock().await;
    let Some(mut entry) = guard.remove(user) else {
        return (StatusCode::NOT_FOUND, "not running").into_response();
    };
    let _ = entry.child.start_kill();
    let _ = entry.child.wait().await;
    (StatusCode::OK, format!("stopped {}", user)).into_response()
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
    let mut out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if out.is_empty() {
        out.push_str("file");
    }
    // Avoid path traversal
    while out.contains('/') {
        out = out.replace('/', "-");
    }
    out
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
    format!(
        "gui-{}-{:02x}{:02x}{:02x}{:02x}",
        now_nanos(),
        r[0],
        r[1],
        r[2],
        r[3]
    )
}

fn get_data_dir() -> PathBuf {
    #[cfg(windows)]
    {
        if let Ok(local) = env::var("LOCALAPPDATA") {
            return PathBuf::from(local).join("CloudP2P");
        }
    }
    #[cfg(not(windows))]
    {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(".cloudp2p");
        }
    }
    PathBuf::from(".cloudp2p")
}
