use clap::{Parser, ValueEnum};
use cloud_p2p_raft::data_structures::Command;
use rand::{distributions::Alphanumeric, Rng};
use std::{fs, net::SocketAddr, str::FromStr, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::sleep,
};

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
    #[arg(long, value_enum, default_value_t = FirstTry::Broadcast)]
    first_try: FirstTry,

    /// Max retries after failures/NOT_LEADER
    #[arg(long, default_value_t = 8)]
    max_retries: usize,

    /// Initial backoff in ms
    #[arg(long, default_value_t = 150)]
    backoff_ms: u64,

    /// Command to execute (e.g., SendPhoto)
    #[arg(long)]
    command: Option<String>,

    /// Path to the photo file
    #[arg(long)]
    photo_path: Option<String>,

    /// ID of the target node
    #[arg(long)]
    target_node: Option<String>,
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

    // If command-line arguments for command, photo_path, and target_node are provided, execute the command immediately
    if let Some(command) = args.command {
        match command.as_str() {
            "SendPhoto" => {
                let photo_path = args.photo_path.as_deref().unwrap_or("");
                let target_node = args.target_node.as_deref().unwrap_or("");

                // Read the photo file
                let photo_data = fs::read(photo_path).expect("Failed to read photo file");

                // Create the SendPhoto command
                let photo_id = "test_photo".to_string(); // Generate a unique ID in a real implementation
                let command = Command::SendPhoto {
                    photo_id,
                    photo_data,
                };

                // Forward the command to the target node
                forward_command_to_node(command, target_node).await?;
            }
            _ => {
                eprintln!("Unsupported command: {}", command);
            }
        }
    } else {
        // Regular proxy operation
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

    Ok(())
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

    // Present a simple banner (your proxy protocol)
    w.write_all(b"Welcome to Cloud P2P Proxy!\n").await?;
    w.write_all(b"Commands: REGISTER <user> <ip> | UNREGISTER <user> | SHOW_USERS | LIST | LEADER\n").await?;

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
                // mutate with idempotent SUBMIT
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
                // read-only; ask any node until success, return raw (multi-line)
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
                // Try to discover the leader by asking nodes
                match find_leader(&seeds).await {
                    Ok(Some(line)) => w.write_all(line.as_bytes()).await?,
                    Ok(None) => w.write_all(b"NO_LEADER\n").await?,
                    Err(e) => w.write_all(format!("ERR {}\n", e).as_bytes()).await?,
                }
            }

            "SEND_PHOTO" => {
                let photo_id = match parts.next() {
                    Some(x) => x,
                    None => {
                        w.write_all(b"Usage: SEND_PHOTO <photo_id>\n").await?;
                        continue;
                    }
                };

                // Read binary data from the client
                let mut photo_data = Vec::new();
                reader.read_to_end(&mut photo_data).await?;

                let op_id = next_op_id();
                let payload = Command::SendPhoto {
                    photo_id: photo_id.to_string(),
                    photo_data,
                };

                // Serialize the command and submit it
                let serialized = serde_json::to_string(&payload)?;
                let resp = submit_idempotent(&seeds, &cfg, &serialized).await;
                write_line(&mut w, resp).await?;
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
     resp: Result<String, anyhow::Error>
 ) -> anyhow::Result<()> {
    match resp {
        Ok(s) => {
            // We expect single-line like "OK\n" or "REDIRECT ...\n"
            w.write_all(s.as_bytes()).await?;
        }
        Err(e) => {
            w.write_all(format!("ERR {}\n", e).as_bytes()).await?;
        }
    }
    Ok(())
}

/* ============================
   Core forwarding helpers
   ============================ */

/// End-to-end idempotent submit with initial try + redirect + retries + backoff.
async fn submit_idempotent(
    seeds: &[SocketAddr],
    cfg: &ProxyCfg,
    cmd_line: &str,
) -> Result<String, anyhow::Error> {
    // initial attempt
    let mut last_err: Option<anyhow::Error> = None;

    match cfg.first_try {
        FirstTry::Broadcast => {
            // fire concurrently; first OK wins
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
                    Ok(Resp::NotLeader) => { /* wait for any OK from others */ }
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
                    Ok(Resp::NotLeader) => { /* try next */ }
                    Ok(Resp::Other(s)) => return Ok(s),
                    Err(e) => last_err = Some(e.into()),
                }
            }
        }
    }

    // retries with backoff (handles leader failover)
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

/// Send a one-off command to a specific node and parse the first line.
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

/// If redirected, reconnect to that address and resend once.
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
    Ok(Resp::Other(line)) // For LIST/SHOW_USERS we may get multi-line; caller will handle.
}

/// Read-only helper: try each seed until one returns something.
/// This reads *until EOF or socket close*, which works with your current node's single-line loop output.
async fn query_any(seeds: &[SocketAddr], cmd_line: &str) -> anyhow::Result<String> {
    let mut last_err: Option<anyhow::Error> = None;

    for &addr in seeds {
        if let Ok(s) = read_multiline(addr, cmd_line).await {
            return Ok(s);
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

    // We don't know number of lines; read with small delay until idle.
    let mut out = String::new();
    loop {
        let mut buf = String::new();
        tokio::select! {
            n = reader.read_line(&mut buf) => {
                let n = n?;
                if n == 0 { break; }
                out.push_str(&buf);
                // For nodes that print just one line (e.g., "(empty)\n"), we can break when it looks done.
                if cmd_line == "LEADER" || cmd_line == "LIST" || cmd_line == "SHOW_USERS" {
                    // Heuristic: if we got one or more lines and the next read would hang, caller can live with the partial.
                    // We'll rely on client to close or keep reading; here we return what we got immediately.
                    // (Nodes currently write all lines in a burst.)
                    if !buf.is_empty() { break; }
                }
            }
            _ = sleep(Duration::from_millis(100)) => {
                // brief idle; return what we have
                break;
            }
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

async fn forward_command_to_node(command: Command, target_node: &str) -> anyhow::Result<()> {
    // Serialize and send the command to the target node
    println!("Forwarding {:?} to node {}", command, target_node);
    // Add networking logic here
    Ok(())
}
