// proxy.rs
// use clap::{Parser, ValueEnum};
// use rand::{distributions::Alphanumeric, Rng};
// use std::{
//     net::SocketAddr,
//     str::FromStr,
//     time::Duration,
//     fs,
//     path::Path,
// };
// use tokio::{
//     io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
//     net::{TcpListener, TcpStream},
//     time::sleep,
// };

// use tokio::{
//     io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
//     net::{TcpStream, tcp::ReadHalf},
//     time::{sleep, Duration},
// };
// use std::{fs, net::SocketAddr};
// use anyhow::Result;
// use rand::{Rng, distributions::Alphanumeric};

use anyhow::Result;

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
    net::{TcpListener, TcpStream, tcp::ReadHalf},
    time::{sleep},
};


use cloud_p2p_raft::crypto::{extract_payload, decrypt_bytes};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Proxy listen address (what your users connect to), e.g. 0.0.0.0:9100
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: String,

    /// Comma-separated list of seed client API addresses (node-facing), e.g. 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
    #[arg(long)]
    seeds: String,

    /// First attempt strategy
    #[arg(long, value_enum, default_value_t = FirstTry::Sequential)]
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
    println!("   Strategy: {:?}", args.first_try);
    println!("   Max retries: {}", args.max_retries);

    loop {
        let (stream, peer) = listener.accept().await?;
        println!("📡 Client connected: {}", peer);

        let seeds = seeds.clone();
        let cfg = ProxyCfg {
            first_try: args.first_try,
            max_retries: args.max_retries,
            backoff_ms: args.backoff_ms,
        };

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, seeds, cfg).await {
                eprintln!("❌ Client handler error from {}: {e:?}", peer);
            }
            println!("👋 Client {} disconnected", peer);
        });
    }
}

struct ProxyCfg {
    first_try: FirstTry,
    max_retries: usize,
    backoff_ms: u64,
}

async fn handle_client(stream: TcpStream, seeds: Vec<SocketAddr>, cfg: ProxyCfg) -> anyhow::Result<()> {
    let peer_addr = stream.peer_addr()?;
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut line = String::new();

    w.write_all(b"Welcome to Cloud P2P Proxy!\n").await?;
    w.write_all(b"Commands: REGISTER <user> <ip> | UNREGISTER <user> | SHOW_USERS | LIST | LEADER | ENCRYPT_IMAGE <id> <passphrase> <input> <output> | ENCRYPT_ON_CLOUD <image_id> <passphrase> | DECRYPT_IMAGE <passphrase> <stego_png> <output>\n").await?;

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

        println!("📨 Client {}: {}", peer_addr, raw);

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
                println!("🚀 Submitting: {}", payload);
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
                println!("🚀 Submitting: {}", payload);
                let resp = submit_idempotent(&seeds, &cfg, &payload).await;
                write_line(&mut w, resp).await?;
            }

            "SHOW_USERS" => {
                println!("📖 Querying SHOW_USERS");
                match query_any(&seeds, "SHOW_USERS").await {
                    Ok(s) => w.write_all(s.as_bytes()).await?,
                    Err(e) => {
                        eprintln!("❌ SHOW_USERS failed: {}", e);
                        w.write_all(format!("ERR {}\n", e).as_bytes()).await?;
                    }
                }
            }

            "LIST" => {
                println!("📖 Querying LIST");
                match query_any(&seeds, "LIST").await {
                    Ok(s) => w.write_all(s.as_bytes()).await?,
                    Err(e) => {
                        eprintln!("❌ LIST failed: {}", e);
                        w.write_all(format!("ERR {}\n", e).as_bytes()).await?;
                    }
                }
            }

            "LEADER" => {
                println!("🔍 Finding leader");
                match find_leader(&seeds).await {
                    Ok(Some(line)) => w.write_all(line.as_bytes()).await?,
                    Ok(None) => w.write_all(b"NO_LEADER\n").await?,
                    Err(e) => {
                        eprintln!("❌ LEADER query failed: {}", e);
                        w.write_all(format!("ERR {}\n", e).as_bytes()).await?;
                    }
                }
            }

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
                println!("🔐 Submitting encryption: {}", payload);
                let resp = submit_idempotent(&seeds, &cfg, &payload).await;
                write_line(&mut w, resp).await?;
            }

            "ENCRYPT_ON_CLOUD" => {
                let image_id = match parts.next() {
                    Some(x) if !x.is_empty() => x,
                    _ => { w.write_all(b"Usage: ENCRYPT_ON_CLOUD <image_id> <passphrase>\n").await?; continue; }
                };
                let pass = match parts.next() {
                    Some(x) => x,
                    _ => { w.write_all(b"Usage: ENCRYPT_ON_CLOUD <image_id> <passphrase>\n").await?; continue; }
                };

                // Validate image_id format
                if !image_id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
                    w.write_all(b"ERR invalid image_id: use alphanumeric, dash, underscore only\n").await?;
                    continue;
                }

                let input_path = match find_upload_by_prefix("uploads", image_id) {
                    Ok(p) => {
                        println!("📁 Found upload: {}", p);
                        p
                    }
                    Err(e) => {
                        eprintln!("❌ Upload not found for '{}': {}", image_id, e);
                        w.write_all(format!("ERR input not found: {}\n", e).as_bytes()).await?;
                        continue;
                    }
                };

                let output_path = format!("stego/{}.png", image_id);
                if let Some(parent) = Path::new(&output_path).parent() {
                    let _ = fs::create_dir_all(parent);
                }

                let op_id = next_op_id();
                let payload = format!("SUBMIT {} ENCRYPT_IMAGE {} {} {} {}",
                    op_id, image_id, pass, input_path, output_path);

                println!("🔐 Submitting cloud encryption for {}", image_id);
                let resp = submit_idempotent(&seeds, &cfg, &payload).await;
                write_line(&mut w, resp).await?;
            }

            "DECRYPT_IMAGE" => {
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

                println!("🔓 Decrypting {} -> {}", stego_path, output_path);

                let bytes = match fs::read(stego_path) {
                    Ok(b) => b,
                    Err(e) => { 
                        eprintln!("❌ Read stego failed: {}", e);
                        w.write_all(format!("ERR read stego: {e}\n").as_bytes()).await?; 
                        continue; 
                    }
                };

                let img = match image::load_from_memory(&bytes) {
                    Ok(i) => i.to_rgba8(),
                    Err(e) => { 
                        eprintln!("❌ Decode PNG failed: {}", e);
                        w.write_all(format!("ERR decode stego PNG: {e}\n").as_bytes()).await?; 
                        continue; 
                    }
                };

                let (nonce, ciphertext) = match extract_payload(&img) {
                    Ok(t) => t,
                    Err(e) => { 
                        eprintln!("❌ Extract payload failed: {}", e);
                        w.write_all(format!("ERR extract payload: {e}\n").as_bytes()).await?; 
                        continue; 
                    }
                };

                let plaintext = match decrypt_bytes(pass.as_bytes(), &nonce, &ciphertext) {
                    Ok(p) => p,
                    Err(e) => { 
                        eprintln!("❌ Decrypt failed: {}", e);
                        w.write_all(format!("ERR decrypt embedded ciphertext: {e}\n").as_bytes()).await?; 
                        continue; 
                    }
                };

                if let Some(parent) = Path::new(output_path).parent() {
                    if let Err(e) = fs::create_dir_all(parent) {
                        eprintln!("❌ Create dir failed: {}", e);
                        w.write_all(format!("ERR create_dir_all {:?}: {e}\n", parent).as_bytes()).await?;
                        continue;
                    }
                }

                if let Err(e) = fs::write(output_path, &plaintext) {
                    eprintln!("❌ Write output failed: {}", e);
                    w.write_all(format!("ERR write output: {e}\n").as_bytes()).await?;
                    continue;
                }

                println!("✅ Decryption successful: {}", output_path);
                w.write_all(b"OK\n").await?;
            }

            _ => {
                eprintln!("❌ Unknown command: {}", cmd);
                w.write_all(format!("ERR unknown command: {}\n", cmd).as_bytes()).await?;
            }
        }
    }

    Ok(())
}

async fn write_line(
     w: &mut tokio::net::tcp::OwnedWriteHalf,
     resp: Result<String, anyhow::Error>
 ) -> anyhow::Result<()> {
    match resp {
        Ok(s) => {
            w.write_all(s.as_bytes()).await?;
        }
        Err(e) => {
            eprintln!("❌ Request failed: {}", e);
            w.write_all(format!("ERR {}\n", e).as_bytes()).await?;
        }
    }
    Ok(())
}

/* ============================
   Core forwarding helpers
   ============================ */
// use tokio::{
//     io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
//     net::{TcpStream, tcp::ReadHalf},
//     time::{sleep, Duration},
// };
// use std::{fs, net::SocketAddr};
// use anyhow::Result;
// use rand::{Rng, distributions::Alphanumeric};

async fn submit_idempotent(
    seeds: &[SocketAddr],
    cfg: &ProxyCfg,
    cmd_line: &str,
) -> Result<String> {
    let mut errors = Vec::new();

    let is_write = cmd_line.contains("SUBMIT");
    let strategy = if is_write {
        FirstTry::Sequential
    } else {
        cfg.first_try
    };

    match strategy {
        FirstTry::Broadcast => {
            let mut tasks = Vec::new();
            for &addr in seeds {
                let line = cmd_line.to_string();
                tasks.push(tokio::spawn(async move { talk_once(addr, &line).await }));
            }
            for t in tasks {
                match t.await?? {
                    Resp::Ok(s) => return Ok(s),
                    Resp::Redirect(to) => {
                        if let Ok(s) = send_once(&to, cmd_line).await {
                            return Ok(s);
                        }
                    }
                    Resp::NotLeader => {}
                    Resp::Other(s) => return Ok(s),
                }
            }
        }
        FirstTry::Sequential => {
            for &addr in seeds {
                match talk_once(addr, cmd_line).await {
                    Ok(Resp::Ok(s)) => return Ok(s),
                    Ok(Resp::Redirect(to)) => {
                        if let Ok(s) = send_once(&to, cmd_line).await {
                            return Ok(s);
                        }
                    }
                    Ok(Resp::NotLeader) => {}
                    Ok(Resp::Other(s)) => return Ok(s),
                    Err(e) => errors.push(format!("{}: {}", addr, e)),
                }
            }
        }
    }

    let mut backoff = Duration::from_millis(cfg.backoff_ms);

    for retry in 0..cfg.max_retries {
        for &addr in seeds {
            match talk_once(addr, cmd_line).await {
                Ok(Resp::Ok(s)) => return Ok(s),
                Ok(Resp::Redirect(to)) => {
                    if let Ok(s) = send_once(&to, cmd_line).await {
                        return Ok(s);
                    }
                }
                Ok(Resp::NotLeader) => {}
                Ok(Resp::Other(s)) => return Ok(s),
                Err(e) => errors.push(format!("{}: {}", addr, e)),
            }
        }

        let jitter = rand::thread_rng().gen_range(0..(backoff.as_millis() as u64 / 3 + 1));
        let sleep_duration = backoff + Duration::from_millis(jitter);
        sleep(sleep_duration).await;

        backoff = std::cmp::min(backoff * 2, Duration::from_secs(2));
    }

    Err(anyhow::anyhow!("All attempts failed: {}", errors.join("; ")))
}

async fn talk_once(addr: SocketAddr, cmd_line: &str) -> Result<Resp> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(addr)
    )
    .await
    .map_err(|_| anyhow::anyhow!("connection timeout"))??;

    // Banner read BEFORE split
    {
        let mut banner_reader = BufReader::new(&mut stream);
        let mut tmp = String::new();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
        tmp.clear();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
    }

    // Now split
    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    parse_first_line(&mut reader).await
}

async fn send_once(to: &str, cmd_line: &str) -> Result<String> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(to.parse::<SocketAddr>()?)
    )
    .await??;

    // Banner
    {
        let mut banner_reader = BufReader::new(&mut stream);
        let mut tmp = String::new();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
        tmp.clear();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
    }

    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut resp = String::new();
    tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut resp)).await??;
    Ok(resp)
}

enum Resp {
    Ok(String),
    Redirect(String),
    NotLeader,
    Other(String),
}

async fn parse_first_line(
    reader: &mut BufReader<ReadHalf<'_>>
) -> Result<Resp> {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line)).await??;

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

async fn query_any(seeds: &[SocketAddr], cmd_line: &str) -> Result<String> {
    let mut errors = Vec::new();
    for &addr in seeds {
        match read_multiline(addr, cmd_line).await {
            Ok(s) => return Ok(s),
            Err(e) => errors.push(format!("{}: {}", addr, e)),
        }
    }
    Err(anyhow::anyhow!("No node responded: {}", errors.join("; ")))
}

async fn read_multiline(addr: SocketAddr, cmd_line: &str) -> Result<String> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(addr)
    )
    .await??;

    // Banner
    {
        let mut banner_reader = BufReader::new(&mut stream);
        let mut tmp = String::new();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
        tmp.clear();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
    }

    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let base_timeout_ms: u64 = if cmd_line.contains("ENCRYPT_IMAGE") {
        10000
    } else if cmd_line == "LEADER" {
        500
    } else {
        2000
    };

    let mut out = String::new();
    let start = tokio::time::Instant::now();

    loop {
        let mut buf = String::new();

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let remaining_ms = base_timeout_ms.saturating_sub(elapsed_ms);
        if remaining_ms == 0 {
            break;
        }

        tokio::select! {
            result = reader.read_line(&mut buf) => {
                let n = result?;
                if n == 0 { break; }
                out.push_str(&buf);

                let trimmed = buf.trim();
                if trimmed == "OK"
                    || trimmed.starts_with("ERR")
                    || trimmed.starts_with("REDIRECT")
                    || trimmed == "NOT_LEADER"
                    || trimmed == "(empty)"
                {
                    break;
                }

                if cmd_line == "LEADER" && !out.is_empty() {
                    break;
                }
            }
            _ = sleep(Duration::from_millis(remaining_ms)) => break,
        }
    }

    if out.is_empty() {
        anyhow::bail!("empty reply from {}", addr);
    }

    Ok(out)
}

async fn find_leader(seeds: &[SocketAddr]) -> Result<Option<String>> {
    for &addr in seeds {
        if let Ok(line) = read_one_line(addr, "LEADER").await {
            if line.starts_with("LEADER ") {
                return Ok(Some(line));
            }
        }
    }
    Ok(None)
}

async fn read_one_line(addr: SocketAddr, cmd_line: &str) -> Result<String> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(3),
        TcpStream::connect(addr)
    )
    .await??;

    {
        let mut banner_reader = BufReader::new(&mut stream);
        let mut tmp = String::new();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
        tmp.clear();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
    }

    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);

    w.write_all(cmd_line.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut resp = String::new();
    tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut resp)).await??;
    Ok(resp)
}

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

fn find_upload_by_prefix(dir: &str, image_id_prefix: &str) -> Result<String> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&format!("{}-", image_id_prefix)) {
            return Ok(entry.path().to_string_lossy().to_string());
        }
    }
    Err(anyhow::anyhow!("no file starting with '{}-'", image_id_prefix))
}
