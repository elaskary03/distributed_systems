use clap::Parser;
use rand::{distributions::Alphanumeric, Rng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about = "Load testing client for Cloud P2P Raft")]
struct Args {
    /// Proxy address to connect to
    #[arg(long, default_value = "127.0.0.1:9100")]
    proxy: String,

    /// Number of concurrent clients
    #[arg(long, default_value = "100")]
    clients: usize,

    /// Number of requests per client
    #[arg(long, default_value = "10")]
    requests: usize,

    /// Delay between requests (ms)
    #[arg(long, default_value = "100")]
    delay_ms: u64,

    /// Test mode: register, unregister, query, mixed
    #[arg(long, default_value = "mixed")]
    mode: String,

    /// Keep connections alive (reuse same connection)
    #[arg(long)]
    keep_alive: bool,

    /// Ramp-up time (seconds) - gradually increase clients
    #[arg(long, default_value = "0")]
    ramp_up_secs: u64,
}

struct Stats {
    total_requests: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,
    timeouts: AtomicU64,
    redirects: AtomicU64,
    latency_sum_ms: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            redirects: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
        }
    }

    fn print_summary(&self) {
        let total = self.total_requests.load(Ordering::Relaxed);
        let success = self.successful.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let timeouts = self.timeouts.load(Ordering::Relaxed);
        let redirects = self.redirects.load(Ordering::Relaxed);
        let latency_sum = self.latency_sum_ms.load(Ordering::Relaxed);
        let avg_latency = if success > 0 {
            latency_sum / success
        } else {
            0
        };

        println!("\n╔════════════════════════════════════════╗");
        println!("║          LOAD TEST SUMMARY             ║");
        println!("╠════════════════════════════════════════╣");
        println!("║ Total Requests:     {:>18} ║", total);
        println!("║ Successful:         {:>18} ║", success);
        println!("║ Failed:             {:>18} ║", failed);
        println!("║ Timeouts:           {:>18} ║", timeouts);
        println!("║ Redirects:          {:>18} ║", redirects);
        println!("║ Success Rate:       {:>17.2}% ║", (success as f64 / total as f64) * 100.0);
        println!("║ Avg Latency:        {:>15} ms ║", avg_latency);
        println!("╚════════════════════════════════════════╝\n");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let stats = Arc::new(Stats::new());

    println!("🚀 Starting Load Test");
    println!("   Proxy: {}", args.proxy);
    println!("   Clients: {}", args.clients);
    println!("   Requests per client: {}", args.requests);
    println!("   Mode: {}", args.mode);
    println!("   Keep-alive: {}", args.keep_alive);
    println!("   Ramp-up: {}s", args.ramp_up_secs);
    println!();

    let start_time = Instant::now();
    let mut tasks = Vec::new();

    // Calculate delay between spawning clients for ramp-up
    let spawn_delay_ms = if args.ramp_up_secs > 0 && args.clients > 1 {
        (args.ramp_up_secs * 1000) / (args.clients as u64 - 1)
    } else {
        0
    };

    for client_id in 0..args.clients {
        let proxy = args.proxy.clone();
        let mode = args.mode.clone();
        let stats = stats.clone();
        let requests = args.requests;
        let delay_ms = args.delay_ms;
        let keep_alive = args.keep_alive;

        // Ramp-up delay
        if spawn_delay_ms > 0 {
            sleep(Duration::from_millis(spawn_delay_ms)).await;
        }

        let task = tokio::spawn(async move {
            if let Err(e) = run_client(
                client_id,
                &proxy,
                &mode,
                requests,
                delay_ms,
                keep_alive,
                stats,
            )
            .await
            {
                eprintln!("❌ Client {} error: {:?}", client_id, e);
            }
        });

        tasks.push(task);

        // Print progress every 10 clients
        if (client_id + 1) % 10 == 0 {
            println!("📊 Spawned {}/{} clients...", client_id + 1, args.clients);
        }
    }

    println!("⏳ Waiting for all clients to complete...\n");

    // Progress reporter
    let stats_clone = stats.clone();
    let total_expected = (args.clients * args.requests) as u64;
    let reporter = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(2)).await;
            let completed = stats_clone.total_requests.load(Ordering::Relaxed);
            let success = stats_clone.successful.load(Ordering::Relaxed);
            let failed = stats_clone.failed.load(Ordering::Relaxed);
            
            println!(
                "📈 Progress: {}/{} ({:.1}%) | Success: {} | Failed: {}",
                completed,
                total_expected,
                (completed as f64 / total_expected as f64) * 100.0,
                success,
                failed
            );

            if completed >= total_expected {
                break;
            }
        }
    });

    // Wait for all clients to finish
    for task in tasks {
        let _ = task.await;
    }

    reporter.abort();

    let elapsed = start_time.elapsed();
    println!("\n✅ All clients completed in {:.2}s", elapsed.as_secs_f64());

    stats.print_summary();

    Ok(())
}

async fn run_client(
    client_id: usize,
    proxy: &str,
    mode: &str,
    requests: usize,
    delay_ms: u64,
    keep_alive: bool,
    stats: Arc<Stats>,
) -> anyhow::Result<()> {
    if keep_alive {
        // Single persistent connection - robust implementation using owned halves
        match connect_to_proxy_keepalive(proxy).await {
            Ok((mut reader, mut writer)) => {
                println!("🔗 Client {} established persistent connection", client_id);

                for req_id in 0..requests {
                    let cmd = generate_command(mode, client_id, req_id);
                    execute_request_keepalive(&mut reader, &mut writer, &cmd, stats.clone()).await;

                    if delay_ms > 0 {
                        sleep(Duration::from_millis(delay_ms)).await;
                    }
                }

                println!("✅ Client {} completed {} requests", client_id, requests);
            }
            Err(e) => {
                eprintln!("❌ Client {} initial connection failed: {}", client_id, e);
                // mark all requests as failed
                for _ in 0..requests {
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                    stats.total_requests.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    } else {
        // New connection per request
        for req_id in 0..requests {
            let mut stream = match connect_to_proxy(proxy).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("❌ Client {} connection failed: {}", client_id, e);
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                    stats.total_requests.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            
            let cmd = generate_command(mode, client_id, req_id);
            execute_request(&mut stream, &cmd, stats.clone()).await;
            
            if delay_ms > 0 {
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }

    Ok(())
}

async fn connect_to_proxy(proxy: &str) -> anyhow::Result<TcpStream> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(proxy),
    )
    .await??;

    // Read banner (2 lines)
    let mut reader = BufReader::new(&mut stream);

    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line)).await??;
    line.clear();
    tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line)).await??;

    Ok(stream)
}

/// Connect and return owned read/write halves for keepalive usage.
/// We read the banner before splitting to avoid losing banner data.
async fn connect_to_proxy_keepalive(proxy: &str) -> anyhow::Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf)> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(proxy),
    )
    .await??;

    // Read banner lines from &mut stream before splitting
    {
        let mut banner_reader = BufReader::new(&mut stream);
        let mut tmp = String::new();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
        tmp.clear();
        tokio::time::timeout(Duration::from_secs(2), banner_reader.read_line(&mut tmp)).await??;
    }

    // Split into owned halves and return a BufReader for the read half
    let (r, w) = stream.into_split();
    Ok((BufReader::new(r), w))
}

async fn execute_request_keepalive(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    cmd: &str,
    stats: Arc<Stats>,
) {
    let start = Instant::now();
    stats.total_requests.fetch_add(1, Ordering::Relaxed);

    // Send command
    if let Err(e) = writer.write_all(cmd.as_bytes()).await {
        eprintln!("❌ Write failed: {}", e);
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if let Err(e) = writer.write_all(b"\n").await {
        eprintln!("❌ Write newline failed: {}", e);
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if let Err(e) = writer.flush().await {
        eprintln!("❌ Flush failed: {}", e);
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // Read response with timeout
    let mut response = String::new();
    match tokio::time::timeout(
        Duration::from_secs(10),
        reader.read_line(&mut response)
    )
    .await
    {
        Ok(Ok(n)) if n > 0 => {
            let latency_ms = start.elapsed().as_millis() as u64;
            stats.latency_sum_ms.fetch_add(latency_ms, Ordering::Relaxed);

            let trimmed = response.trim();
            if trimmed == "OK" {
                stats.successful.fetch_add(1, Ordering::Relaxed);
            } else if trimmed.starts_with("REDIRECT") {
                stats.redirects.fetch_add(1, Ordering::Relaxed);
                stats.successful.fetch_add(1, Ordering::Relaxed);
            } else if trimmed.starts_with("ERR") {
                eprintln!("⚠️  Command '{}' returned: {}", cmd.trim(), trimmed);
                stats.failed.fetch_add(1, Ordering::Relaxed);
            } else {
                // Other responses (LEADER, user list, etc.)
                stats.successful.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(Ok(_)) => {
            eprintln!("⚠️  Empty response for: {}", cmd.trim());
            stats.failed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Err(e)) => {
            eprintln!("❌ Read error: {}", e);
            stats.failed.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            eprintln!("⏱️  Timeout for: {}", cmd.trim());
            stats.timeouts.fetch_add(1, Ordering::Relaxed);
            stats.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn execute_request(stream: &mut TcpStream, cmd: &str, stats: Arc<Stats>) {
    let start = Instant::now();
    stats.total_requests.fetch_add(1, Ordering::Relaxed);

    // For non-keepalive path we can use a temporary split (per-connection)
    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);

    // Send command
    if let Err(e) = w.write_all(cmd.as_bytes()).await {
        eprintln!("❌ Write failed: {}", e);
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if let Err(e) = w.write_all(b"\n").await {
        eprintln!("❌ Write newline failed: {}", e);
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }

    // Read response with timeout
    let mut response = String::new();
    match tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut response)).await {
        Ok(Ok(n)) if n > 0 => {
            let latency_ms = start.elapsed().as_millis() as u64;
            stats.latency_sum_ms.fetch_add(latency_ms, Ordering::Relaxed);

            let trimmed = response.trim();
            if trimmed == "OK" {
                stats.successful.fetch_add(1, Ordering::Relaxed);
            } else if trimmed.starts_with("REDIRECT") {
                stats.redirects.fetch_add(1, Ordering::Relaxed);
                stats.successful.fetch_add(1, Ordering::Relaxed);
            } else if trimmed.starts_with("ERR") {
                eprintln!("⚠️  Command '{}' returned: {}", cmd.trim(), trimmed);
                stats.failed.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.successful.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(Ok(_)) => {
            eprintln!("⚠️  Empty response for: {}", cmd.trim());
            stats.failed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Err(e)) => {
            eprintln!("❌ Read error: {}", e);
            stats.failed.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            eprintln!("⏱️  Timeout for: {}", cmd.trim());
            stats.timeouts.fetch_add(1, Ordering::Relaxed);
            stats.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn generate_command(mode: &str, client_id: usize, req_id: usize) -> String {
    let rand_suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(4)
        .map(char::from)
        .collect();

    match mode {
        "register" => {
            format!(
                "REGISTER user_c{}_r{}_{} 192.168.{}.{}",
                client_id, req_id, rand_suffix,
                (client_id % 255), ((req_id % 255) + 1)
            )
        }
        "unregister" => {
            format!("UNREGISTER user_c{}_r{}_{}", client_id, req_id, rand_suffix)
        }
        "query" => {
            match req_id % 3 {
                0 => "SHOW_USERS".into(),
                1 => "LIST".into(),
                2 => "LEADER".into(),
                _ => unreachable!(),
            }
        }
        "mixed" => {
            match req_id % 4 {
                0 => format!(
                    "REGISTER user_c{}_r{}_{} 192.168.{}.{}",
                    client_id, req_id, rand_suffix,
                    (client_id % 255), ((req_id % 255) + 1)
                ),
                1 => "SHOW_USERS".into(),
                2 => "LEADER".into(),
                3 => "LIST".into(),
                _ => unreachable!(),
            }
        }
        _ => "LEADER".into(),
    }
}
