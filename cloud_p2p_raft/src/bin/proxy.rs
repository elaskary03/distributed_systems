use clap::{Parser, ValueEnum};
use rand::{distributions::Alphanumeric, Rng};
use std::{
    fs,
    net::SocketAddr,
    path::Path,
    str::FromStr,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::sleep,
};

use cloud_p2p_raft::crypto::{decrypt_bytes, encrypt_and_embed_to_png, extract_payload};
use image::GenericImageView; // optional
use reqwest::multipart::{Form, Part};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Proxy listen address (what the GUI connects to), e.g. 0.0.0.0:9100
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: String,

    /// Comma-separated list of seed client API addresses (node-facing), e.g. 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
    #[arg(long)]
    seeds: String,

    /// First attempt strategy
    #[arg(long, value_enum, default_value_t = FirstTry::Broadcast)]
    first_try: FirstTry,

    /// Max retries after failures/NOT_LEADER
    #[arg(long, default_value_t = 8)]
    max_retries: usize,

    /// Initial backoff in ms
    #[arg(long, default_value_t = 150)]
    backoff_ms: u64,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum FirstTry {
    Broadcast,
    Sequential,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let listen_addr: SocketAddr = args.listen.parse()?;
    let seeds: Vec<SocketAddr> = args
        .seeds
        .split(',')
        .map(|s| SocketAddr::from_str(s.trim()))
        .collect::<Result<_, _>>()?;

    let listener = TcpListener::bind(listen_addr).await?;
    println!("🔗 Proxy listening on {}", listen_addr);
    println!("   Using seeds: {:?}", seeds);

    loop {
        let (stream, peer) = listener.accept().await?;
        println!("📡 client connected: {}", peer);

        let seeds = seeds.clone();
        let cfg = ProxyCfg {
            first_try: args.first_try,
            max_retries: args.max_retries,
            backoff_ms: args.backoff_ms,
        };

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, seeds, cfg).await {
                eprintln!("client handler error: {e:?}");
            }
        });
    }
}

struct ProxyCfg {
    first_try: FirstTry,
    max_retries: usize,
    backoff_ms: u64,
}

async fn handle_client(stream: TcpStream, seeds: Vec<SocketAddr>, cfg: ProxyCfg) -> anyhow::Result<()> {
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut line = String::new();

    // Simple banner (proxy protocol)
    w.write_all(b"Welcome to Cloud P2P Proxy!\n").await?;
    w.write_all(b"Commands: REGISTER <user> <ip> | UNREGISTER <user> | SHOW_USERS | LIST | LEADER | ENCRYPT_IMAGE <id> <passphrase> <input> <output> | ENCRYPT_ON_CLOUD <image_id> <passphrase> <input_url> <callback> | DECRYPT_IMAGE <passphrase> <stego_png> <output>\n").await?;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let raw = line.trim();
        if raw.is_empty() {
            continue;
        }

        let mut parts = raw.split_whitespace();
        let cmd = parts.next().unwrap_or_default();

        match cmd {
            "REGISTER" => {
                let user = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: REGISTER <user> <ip>\n").await?; continue; }
                };
                let ip = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: REGISTER <user> <ip>\n").await?; continue; }
                };
                let op_id = next_op_id();
                let payload = format!("SUBMIT {} REGISTER {} {}", op_id, user, ip);
                let resp = submit_idempotent(&seeds, &cfg, &payload).await;
                write_line(&mut w, resp).await?;
            }

            "UNREGISTER" => {
                let user = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: UNREGISTER <user>\n").await?; continue; }
                };
                let op_id = next_op_id();
                let payload = format!("SUBMIT {} UNREGISTER {}", op_id, user);
                let resp = submit_idempotent(&seeds, &cfg, &payload).await;
                write_line(&mut w, resp).await?;
            }

            "SHOW_USERS" => {
                match query_any(&seeds, "SHOW_USERS").await {
                    Ok(s) => w.write_all(s.as_bytes()).await?,
                    Err(e) => w.write_all(format!("ERR {}\n", e).as_bytes()).await?,
                }
            }

            "LIST" => {
                match query_any(&seeds, "LIST").await {
                    Ok(s) => w.write_all(s.as_bytes()).await?,
                    Err(e) => w.write_all(format!("ERR {}\n", e).as_bytes()).await?,
                }
            }

            "LEADER" => {
                match find_leader(&seeds).await {
                    Ok(Some(line)) => w.write_all(line.as_bytes()).await?,
                    Ok(None) => w.write_all(b"NO_LEADER\n").await?,
                    Err(e) => w.write_all(format!("ERR {}\n", e).as_bytes()).await?,
                }
            }

            // Forward to the cluster (old path: requires local files on the node)
            "ENCRYPT_IMAGE" => {
                let id = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: ENCRYPT_IMAGE <id> <passphrase> <input_path> <output_path>\n").await?; continue; }
                };
                let pass = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: ENCRYPT_IMAGE <id> <passphrase> <input_path> <output_path>\n").await?; continue; }
                };
                let input_path = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: ENCRYPT_IMAGE <id> <passphrase> <input_path> <output_path>\n").await?; continue; }
                };
                let output_path = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: ENCRYPT_IMAGE <id> <passphrase> <input_path> <output_path>\n").await?; continue; }
                };

                let op_id = next_op_id();
                let payload = format!("SUBMIT {} ENCRYPT_IMAGE {} {} {} {}", op_id, id, pass, input_path, output_path);
                let resp = submit_idempotent(&seeds, &cfg, &payload).await;
                write_line(&mut w, resp).await?;
            }

            // NEW: proxy performs the encryption itself using a URL + posts back to GUI
            "ENCRYPT_ON_CLOUD" => {
                // ENCRYPT_ON_CLOUD <image_id> <passphrase> <input_url> <callback>
                let image_id = match parts.next() {
                    Some(x) if !x.is_empty() => x,
                    _ => { w.write_all(b"Usage: ENCRYPT_ON_CLOUD <image_id> <passphrase> <input_url> <callback>\n").await?; continue; }
                };
                let pass = match parts.next() {
                    Some(x) => x,
                    _ => { w.write_all(b"Usage: ENCRYPT_ON_CLOUD <image_id> <passphrase> <input_url> <callback>\n").await?; continue; }
                };
                let input_url = match parts.next() {
                    Some(x) => x,
                    _ => { w.write_all(b"Usage: ENCRYPT_ON_CLOUD <image_id> <passphrase> <input_url> <callback>\n").await?; continue; }
                };
                let callback = match parts.next() {
                    Some(x) => x,
                    _ => { w.write_all(b"Usage: ENCRYPT_ON_CLOUD <image_id> <passphrase> <input_url> <callback>\n").await?; continue; }
                };

                // 1) Download original bytes from GUI's public URL
                let original_bytes = match reqwest::get(input_url).await {
                    Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                        Ok(b) => b.to_vec(),
                        Err(e) => { w.write_all(format!("ERR read body: {e}\n").as_bytes()).await?; continue; }
                    },
                    Ok(resp) => {
                        w.write_all(format!("ERR download {} -> HTTP {}\n", input_url, resp.status()).as_bytes()).await?;
                        continue;
                    }
                    Err(e) => { w.write_all(format!("ERR download {}: {e}\n", input_url).as_bytes()).await?; continue; }
                };

                // 2) Read cover image (repo has images/cover_image.png)
                let cover_path = "images/cover_image.png";
                let cover_bytes = match fs::read(cover_path) {
                    Ok(b) => b,
                    Err(e) => { w.write_all(format!("ERR read cover {}: {e}\n", cover_path).as_bytes()).await?; continue; }
                };

                // 3) Encrypt & embed -> PNG bytes
                let (stego_png, sha_hex, _count) = match encrypt_and_embed_to_png(
                    pass.as_bytes(),
                    &original_bytes,
                    &cover_bytes,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        w.write_all(format!("ERR encrypt/embed: {e}\n").as_bytes()).await?;
                        continue;
                    }
                };

                // 4) Save to local stego/<image_id>.png (so GUI can also GET it via /files/stego)
                let out_path = format!("stego/{}.png", image_id);
                if let Some(parent) = Path::new(&out_path).parent() {
                    let _ = fs::create_dir_all(parent);
                }
                if let Err(e) = fs::write(&out_path, &stego_png) {
                    w.write_all(format!("ERR write {}: {e}\n", out_path).as_bytes()).await?;
                    continue;
                }

                // 5) POST back to GUI callback as multipart (image_id, sha256, file)
                let part = reqwest::multipart::Part::bytes(stego_png.clone())
                    .file_name(format!("{}.png", image_id))
                    .mime_str("image/png")
                    .unwrap();
                let form = reqwest::multipart::Form::new()
                    .text("image_id", image_id.to_string())
                    .text("sha256", sha_hex.clone())
                    .part("file", part);

                match reqwest::Client::new().post(callback).multipart(form).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        // fine
                    }
                    Ok(resp) => {
                        w.write_all(format!("ERR callback {} -> HTTP {}\n", callback, resp.status()).as_bytes()).await?;
                        continue;
                    }
                    Err(e) => {
                        w.write_all(format!("ERR callback {}: {e}\n", callback).as_bytes()).await?;
                        continue;
                    }
                }

                w.write_all(b"OK\n").await?;
            }

            "DECRYPT_IMAGE" => {
                // Usage: DECRYPT_IMAGE <passphrase> <stego_png> <output_path>
                let pass = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: DECRYPT_IMAGE <passphrase> <stego_png> <output_path>\n").await?; continue; }
                };
                let stego_path = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: DECRYPT_IMAGE <passphrase> <stego_png> <output_path>\n").await?; continue; }
                };
                let output_path = match parts.next() {
                    Some(x) => x,
                    None => { w.write_all(b"Usage: DECRYPT_IMAGE <passphrase> <stego_png> <output_path>\n").await?; continue; }
                };

                // Read the stego PNG
                let bytes = match fs::read(stego_path) {
                    Ok(b) => b,
                    Err(e) => { w.write_all(format!("ERR read stego: {e}\n").as_bytes()).await?; continue; }
                };

                // Decode image -> RGBA
                let img = match image::load_from_memory(&bytes) {
                    Ok(i) => i.to_rgba8(),
                    Err(e) => { w.write_all(format!("ERR decode stego PNG: {e}\n").as_bytes()).await?; continue; }
                };

                // Extract payload (nonce + ciphertext)
                let (nonce, ciphertext) = match extract_payload(&img) {
                    Ok(t) => t,
                    Err(e) => { w.write_all(format!("ERR extract payload: {e}\n").as_bytes()).await?; continue; }
                };

                // Decrypt
                let plaintext = match decrypt_bytes(pass.as_bytes(), &nonce, &ciphertext) {
                    Ok(p) => p,
                    Err(e) => { w.write_all(format!("ERR decrypt embedded ciphertext: {e}\n").as_bytes()).await?; continue; }
                };

                // Ensure output dir exists
                if let Some(parent) = Path::new(output_path).parent() {
                    if let Err(e) = fs::create_dir_all(parent) {
                        w.write_all(format!("ERR create_dir_all {:?}: {e}\n", parent).as_bytes()).await?;
                        continue;
                    }
                }

                // Write recovered bytes
                if let Err(e) = fs::write(output_path, &plaintext) {
                    w.write_all(format!("ERR write output: {e}\n").as_bytes()).await?;
                    continue;
                }

                w.write_all(b"OK\n").await?;
            }

            _ => {
                w.write_all(format!("ERR unknown command: {}\n", cmd).as_bytes()).await?;
            }
        }
    }

    Ok(())
}

async fn write_line(
    w: &mut tokio::net::tcp::OwnedWriteHalf,
    resp: Result<String, anyhow::Error>,
) -> anyhow::Result<()> {
    match resp {
        Ok(s) => { w.write_all(s.as_bytes()).await?; }
        Err(e) => { w.write_all(format!("ERR {}\n", e).as_bytes()).await?; }
    }
    Ok(())
}

/* ============================
   Core forwarding helpers
   ============================ */

async fn submit_idempotent(
    seeds: &[SocketAddr],
    cfg: &ProxyCfg,
    cmd_line: &str,
) -> Result<String, anyhow::Error> {
    let mut last_err: Option<anyhow::Error> = None;

    match cfg.first_try {
        FirstTry::Broadcast => {
            let mut tasks = Vec::new();
            for &addr in seeds {
                let line = cmd_line.to_string();
                tasks.push(tokio::spawn(async move { talk_once(addr, &line).await }));
            }
            for t in tasks {
                match t.await? {
                    Ok(Resp::Ok(s)) => return Ok(s),
                    Ok(Resp::Redirect(to)) => {
                        if let Ok(s) = send_once(&to, cmd_line).await { return Ok(s); }
                    }
                    Ok(Resp::NotLeader) => {}
                    Ok(Resp::Other(s)) => return Ok(s),
                    Err(e) => last_err = Some(e.into()),
                }
            }
        }
        FirstTry::Sequential => {
            for &addr in seeds {
                match talk_once(addr, cmd_line).await {
                    Ok(Resp::Ok(s)) => return Ok(s),
                    Ok(Resp::Redirect(to)) => {
                        if let Ok(s) = send_once(&to, cmd_line).await { return Ok(s); }
                    }
                    Ok(Resp::NotLeader) => {}
                    Ok(Resp::Other(s)) => return Ok(s),
                    Err(e) => last_err = Some(e.into()),
                }
            }
        }
    }

    let mut backoff = Duration::from_millis(cfg.backoff_ms);
    for _ in 0..cfg.max_retries {
        for &addr in seeds {
            match talk_once(addr, cmd_line).await {
                Ok(Resp::Ok(s)) => return Ok(s),
                Ok(Resp::Redirect(to)) => {
                    if let Ok(s) = send_once(&to, cmd_line).await { return Ok(s); }
                }
                Ok(Resp::NotLeader) => {}
                Ok(Resp::Other(s)) => return Ok(s),
                Err(e) => last_err = Some(e.into()),
            }
        }
        let jitter = rand::thread_rng().gen_range(0..(backoff.as_millis() as u64 / 3 + 1));
        sleep(backoff + Duration::from_millis(jitter)).await;
        backoff = std::cmp::min(backoff * 2, Duration::from_secs(2));
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("submit failed")))
}

async fn talk_once(addr: SocketAddr, cmd_line: &str) -> anyhow::Result<Resp> {
    let mut s = TcpStream::connect(addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // read 2-line banner from node
    let mut tmp = String::new();
    reader.read_line(&mut tmp).await?;
    tmp.clear();
    reader.read_line(&mut tmp).await?;

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    parse_first_line(&mut reader).await
}

async fn send_once(to: &str, cmd_line: &str) -> anyhow::Result<String> {
    let addr: SocketAddr = to.parse()?;
    let mut s = TcpStream::connect(addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // banner
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

enum Resp {
    Ok(String),
    Redirect(String),
    NotLeader,
    Other(String),
}

async fn parse_first_line(reader: &mut BufReader<tokio::net::tcp::ReadHalf<'_>>) -> anyhow::Result<Resp> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let trimmed = line.trim_end();

    if trimmed == "OK" {
        return Ok(Resp::Ok(line));
    }
    if trimmed == "NOT_LEADER" {
        return Ok(Resp::NotLeader);
    }
    if let Some(rest) = trimmed.strip_prefix("REDIRECT ") {
        return Ok(Resp::Redirect(rest.to_string()));
    }
    Ok(Resp::Other(line))
}

async fn query_any(seeds: &[SocketAddr], cmd_line: &str) -> anyhow::Result<String> {
    let mut last_err: Option<anyhow::Error> = None;
    for &addr in seeds {
        match read_multiline(addr, cmd_line).await {
            Ok(s) => return Ok(s),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no node responded")))
}

async fn read_multiline(addr: SocketAddr, cmd_line: &str) -> anyhow::Result<String> {
    let mut s = TcpStream::connect(addr).await?;
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
    loop {
        let mut buf = String::new();
        tokio::select! {
            n = reader.read_line(&mut buf) => {
                let n = n?;
                if n == 0 { break; }
                out.push_str(&buf);
                if cmd_line == "LEADER" { break; }
            }
            _ = sleep(Duration::from_millis(100)) => { break; }
        }
    }
    if out.is_empty() {
        anyhow::bail!("empty reply");
    }
    Ok(out)
}

async fn find_leader(seeds: &[SocketAddr]) -> anyhow::Result<Option<String>> {
    for &addr in seeds {
        if let Ok(line) = read_one_line(addr, "LEADER").await {
            if line.starts_with("LEADER ") {
                return Ok(Some(line));
            }
        }
    }
    Ok(None)
}

async fn read_one_line(addr: SocketAddr, cmd_line: &str) -> anyhow::Result<String> {
    let mut s = TcpStream::connect(addr).await?;
    let (r, mut w) = s.split();
    let mut reader = BufReader::new(r);

    // banner
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

/* ================
   util
   ================ */

fn next_op_id() -> String {
    let rand4: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(4)
        .map(char::from)
        .collect();
    format!("cli-{}-{}", now_nanos(), rand4)
}

fn now_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[allow(dead_code)]
fn find_upload_by_prefix(dir: &str, image_id_prefix: &str) -> anyhow::Result<String> {
    let mut chosen: Option<String> = None;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&format!("{}-", image_id_prefix)) {
            let p = entry.path().to_string_lossy().to_string();
            chosen = Some(p);
            break;
        }
    }
    chosen.ok_or_else(|| anyhow::anyhow!("no file starting with '{}-'", image_id_prefix))
}
