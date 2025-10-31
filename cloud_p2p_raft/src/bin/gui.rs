use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::sleep,
};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let state = AppState {
        proxy_addr: Arc::new(args.proxy_addr),
    };

    let app = Router::new()
        // UI
        .route("/", get(ui))
        // API
        .route("/api/register", post(api_register))
        .route("/api/unregister", post(api_unregister))
        .route("/api/leader", get(api_leader))
        .route("/api/users", get(api_users))
        .route("/api/list", get(api_list))
        // power-user passthrough for any raw line (e.g., "SUBMIT <op_id> ....")
        .route("/api/send", post(api_send))
        .with_state(state);

    let addr: SocketAddr = args.listen.parse()?;
    println!("🖥️  GUI available at http://{}", addr);
    // SAFETY NOTE: Only prints once; actual traffic goes to the proxy below.
    // No changes to your proxy/nodes behavior.
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/* =========================
   UI (single file, embedded)
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
    <h3>Raw Send (power users)</h3>
    <div class="row">
      <input id="raw-line" placeholder='e.g. "LEADER" or "LIST" or "SHOW_USERS"' style="min-width:320px;" />
      <button onclick="rawSend()">Send</button>
    </div>
    <pre id="raw-resp"></pre>
    <div class="muted">Note: For idempotent mutations, the proxy expects <code>SUBMIT &lt;op_id&gt; ...</code>.</div>
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
async function rawSend() {
  const line = document.querySelector('#raw-line').value;
  const r = await fetch('/api/send', {method:'POST', headers:{'Content-Type':'text/plain'}, body: line});
  const t = await r.text();
  document.querySelector('#raw-resp').textContent = t;
}
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
    // Send plain command to the proxy; it will handle idempotent SUBMIT internally.
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
    // If caller sends SUBMIT with op_id, we just relay;
    // otherwise this supports read-only commands like LEADER/LIST/SHOW_USERS.
    if line == "LEADER" || line == "LIST" || line == "SHOW_USERS" {
        proxy_send_multiline(&st.proxy_addr, line).await.into_response()
    } else {
        proxy_send_oneline(&st.proxy_addr, line).await.into_response()
    }
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

/// Connect to the proxy, read its 2-line banner, send one line, read one reply line.
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

/// Read multi-line responses from the proxy:
/// 1) wait up to 1s for the FIRST line (so we don't return empty),
/// 2) then keep reading until 150ms of idle (proxy keeps the socket open).
async fn read_multiline(proxy_addr: &str, cmd_line: &str) -> anyhow::Result<String> {
    use tokio::time::{timeout, Duration};

    let mut s = TcpStream::connect(proxy_addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // banner (2 lines)
    let mut tmp = String::new();
    reader.read_line(&mut tmp).await?;
    tmp.clear();
    reader.read_line(&mut tmp).await?;

    // send
    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut out = String::new();

    // ---- Phase 1: ensure we get at least ONE line (up to 1s)
    let mut first = String::new();
    match timeout(Duration::from_secs(1), reader.read_line(&mut first)).await {
        Ok(Ok(n)) if n > 0 => {
            out.push_str(&first);
            // LEADER is single-line: return early
            if cmd_line == "LEADER" {
                return Ok(out);
            }
        }
        _ => {
            anyhow::bail!("empty reply");
        }
    }

    // ---- Phase 2: collect the rest until short idle
    loop {
        let mut buf = String::new();
        tokio::select! {
            n = reader.read_line(&mut buf) => {
                let n = n?;
                if n == 0 { break; }        // server closed
                out.push_str(&buf);
            }
            _ = sleep(Duration::from_millis(150)) => {
                // brief idle; assume proxy finished writing this burst
                break;
            }
        }
    }

    Ok(out)
}

/* =========================
   small util for op_id
   ========================= */
fn next_op_id() -> String {
    use rand::{distributions::Alphanumeric, Rng};
    fn now_nanos() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }
    let rand4: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(4)
        .map(char::from)
        .collect();
    format!("gui-{}-{}", now_nanos(), rand4)
}
