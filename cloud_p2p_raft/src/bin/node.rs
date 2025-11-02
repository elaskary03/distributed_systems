use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::codec::{LengthDelimitedCodec, FramedRead, FramedWrite};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tracing::{debug, error, info};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use cloud_p2p_raft::crypto::encrypt_and_embed_to_png;
use std::path::Path;
use rand::seq::IteratorRandom;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Node id
    #[arg(long)]
    id: u32,
    /// Listen address (e.g. "192.168.1.100:7001")
    #[arg(long)]
    addr: String,
    /// Peers list: comma separated id=addr (e.g. "2=192.168.1.101:7002,3=192.168.1.102:7003")
    #[arg(long)]
    peers: String,
    /// Client API listen address (e.g. "0.0.0.0:9001")
    #[arg(long, default_value = "127.0.0.1:9000")]
    client_addr: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: u32,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteReply {
        term: u64,
        vote_granted: bool,
        sender_id: u32,
    },
    AppendEntries {
        term: u64,
        leader_id: u32,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendReply {
        term: u64,
        sender_id: u32,
        success: bool,
    },
    LoadReport {
        from_id: u32,
        pending: u32,
        ts_millis: u128,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientRequest {
    Submit { command: String },
    GetLeader,
    GetState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientResponse {
    Ok,
    NotLeader { leader_id: Option<u32> },
    Leader { id: u32 },
    State { role: String, term: u64, commit_index: u64 },
    Error { message: String },
}

#[derive(Default)]
struct Metrics {
    requests_total: AtomicU64,
    failures_total: AtomicU64,
    latency_sum_ns: AtomicU64,
    latency_count: AtomicU64,
    elections_won: AtomicU64,
    tasks_executed_local: AtomicU64,
    tasks_executed_delegated: AtomicU64,
    delegated_sent_total: AtomicU64,
}

#[derive(Clone)]
struct NetNode {
    id: u32,
    state: Arc<RwLock<RaftState>>,
    current_term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<u32>>>,
    peers: Arc<HashMap<u32, SocketAddr>>,
    outbound: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<RaftMessage>>>>,
    last_heartbeat: Arc<RwLock<Instant>>,
    votes_received: Arc<RwLock<HashMap<u64, HashMap<u32, bool>>>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    commit_index: Arc<RwLock<u64>>,
    last_applied: Arc<RwLock<u64>>,
    next_index: Arc<RwLock<HashMap<u32, u64>>>,
    match_index: Arc<RwLock<HashMap<u32, u64>>>,
    leader_hint: Arc<RwLock<Option<u32>>>,
    registered_users: Arc<RwLock<HashMap<String, String>>>,
    processed_ops: Arc<RwLock<std::collections::HashSet<String>>>,
    local_pending: Arc<RwLock<u32>>,
    load_table: Arc<RwLock<HashMap<u32, (u32, u128)>>>,
    client_addr: String,
    metrics: Arc<Metrics>,
}

impl NetNode {
    pub fn new(id: u32, peers: HashMap<u32, SocketAddr>, client_addr: String) -> Self {
        let peers_arc = Arc::new(peers);
        let peer_ids: Vec<u32> = peers_arc.keys().cloned().collect();

        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for peer_id in peer_ids {
            next_index.insert(peer_id, 1);
            match_index.insert(peer_id, 0);
        }

        Self {
            id,
            client_addr,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            peers: peers_arc,
            outbound: Arc::new(Mutex::new(HashMap::new())),
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            votes_received: Arc::new(RwLock::new(HashMap::new())),
            log: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            next_index: Arc::new(RwLock::new(next_index)),
            match_index: Arc::new(RwLock::new(match_index)),
            leader_hint: Arc::new(RwLock::new(None)),
            registered_users: Arc::new(RwLock::new(HashMap::new())),
            processed_ops: Arc::new(RwLock::new(std::collections::HashSet::new())),
            local_pending: Arc::new(RwLock::new(0)),
            load_table: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        }
    }

    #[inline]
    fn clamp1(n: u64) -> u64 {
        if n == 0 { 1 } else { n }
    }

    #[inline]
    fn prev_ptr(next_index: u64, log: &[LogEntry]) -> (u64, u64) {
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            log.get(prev_log_index as usize - 1).map(|e| e.term).unwrap_or(0)
        };
        (prev_log_index, prev_log_term)
    }

    fn client_addr_for(&self, id: u32) -> String {
        if id == self.id {
            return self.client_addr.clone();
        }
        format!(
            "{}:{}",
            self.client_addr.split(':').next().unwrap_or("127.0.0.1"),
            9000 + id
        )
    }

    // ---------- Metrics helpers (no command needed) ----------
    fn metrics_request_start(&self) -> Instant {
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        Instant::now()
    }
    fn metrics_ok(&self, start: Instant) {
        let ns = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.metrics.latency_sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.metrics.latency_count.fetch_add(1, Ordering::Relaxed);
    }
    fn metrics_fail(&self, start: Instant) {
        self.metrics.failures_total.fetch_add(1, Ordering::Relaxed);
        self.metrics_ok(start);
    }

    async fn execute_delegated(&self, cmd: &str) -> anyhow::Result<()> {
        self.execute_locally(cmd).await;
        Ok(())
    }

    async fn forward_to_leader(&self, cmd_line: &str) -> anyhow::Result<String> {
        let lid = (*self.leader_hint.read().await)
            .ok_or_else(|| anyhow::anyhow!("no leader hint"))?;
        let addr = self.client_addr_for(lid);

        let stream = TcpStream::connect(addr).await?;
        let (r, mut w) = stream.into_split();
        let mut reader = BufReader::new(r);

        let mut tmp = String::new();
        reader.read_line(&mut tmp).await?;
        tmp.clear();
        reader.read_line(&mut tmp).await?;

        w.write_all(cmd_line.as_bytes()).await?;
        w.write_all(b"\n").await?;

        use tokio::time::{timeout, Duration};
        let mut out = String::new();

        let mut buf = String::new();
        match timeout(Duration::from_millis(500), reader.read_line(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => out.push_str(&buf),
            _ => return Ok(out),
        }

        loop {
            buf.clear();
            match timeout(Duration::from_millis(120), reader.read_line(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => out.push_str(&buf),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn start(self: Arc<Self>, listen_addr: SocketAddr) -> anyhow::Result<()> {
        let listener = loop {
            match TcpListener::bind(listen_addr).await {
                Ok(l) => {
                    info!("Node {} listening on {}", self.id, listen_addr);
                    break l;
                }
                Err(e) => {
                    error!("Failed to bind to {}: {}. Retrying in 5 seconds...", listen_addr, e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        };

        // stdin input (leader-only)
        {
            let input_node = self.clone();
            tokio::spawn(async move {
                use tokio::io::{self, AsyncBufReadExt};
                let mut lines = io::BufReader::new(io::stdin()).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if matches!(*input_node.state.read().await, RaftState::Leader) {
                        let mut log = input_node.log.write().await;
                        let term = *input_node.current_term.read().await;
                        let index = (log.len() as u64) + 1;
                        log.push(LogEntry { term, index, command: line.clone() });
                        drop(log);

                        info!("Node {}: appended new entry {}", input_node.id, line);

                        let term = *input_node.current_term.read().await;
                        let commit_index = *input_node.commit_index.read().await;
                        let log = input_node.log.read().await;

                        for (&peer_id, _) in input_node.peers.iter() {
                            let next_index = {
                                let next_indices = input_node.next_index.read().await;
                                next_indices.get(&peer_id).copied().unwrap_or(1)
                            };
                            let (prev_log_index, prev_log_term) = NetNode::prev_ptr(next_index, &log);
                            let start_idx = next_index.saturating_sub(1);
                            let start_usize = usize::try_from(start_idx).unwrap_or(0);
                            let entries = if start_usize > log.len() {
                                Vec::new()
                            } else {
                                log[start_usize..].to_vec()
                            };

                            let msg = RaftMessage::AppendEntries {
                                term,
                                leader_id: input_node.id,
                                prev_log_index,
                                prev_log_term,
                                entries,
                                leader_commit: commit_index,
                            };
                            input_node.send_message(peer_id, msg).await;
                        }
                    } else {
                        info!(
                            "Node {}: not leader; ignoring local command '{}'",
                            input_node.id, line
                        );
                    }
                }
            });
        }

        // inbound acceptor
        let node = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("Node {} accepted connection from {}", node.id, addr);
                        let n = node.clone();
                        tokio::spawn(async move {
                            if let Err(e) = n.handle_inbound(stream).await {
                                error!("Inbound handler error from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Accept error: {}. Continuing...", e);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        // outbound connectors with backoff
        for (&peer_id, &addr) in self.peers.iter() {
            let node = self.clone();
            tokio::spawn(async move {
                let mut backoff = Duration::from_secs(1);
                const MAX_BACKOFF: Duration = Duration::from_secs(30);
                loop {
                    info!("Node {} connecting to peer {} at {}", node.id, peer_id, addr);
                    match TcpStream::connect(addr).await {
                        Ok(stream) => {
                            info!("Node {} connected to {} at {}", node.id, peer_id, addr);
                            backoff = Duration::from_secs(1);
                            if let Err(e) = node.handle_outbound(peer_id, stream).await {
                                error!("Outbound {} -> {} failed: {}", node.id, peer_id, e);
                            }
                        }
                        Err(e) => {
                            error!("Connect to {} at {} failed: {}. Retrying in {:?}...", peer_id, addr, e, backoff);
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                        }
                    }
                }
            });
        }

        // reconnect watcher
        let reconnect_node = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                let current_outbound = reconnect_node.outbound.lock().await;
                let connected: Vec<u32> = current_outbound.keys().cloned().collect();
                drop(current_outbound);

                for (&peer_id, &addr) in reconnect_node.peers.iter() {
                    if connected.contains(&peer_id) {
                        continue;
                    }
                    info!("Node {} missing connection to {}, retrying...", reconnect_node.id, peer_id);
                    let node_clone = reconnect_node.clone();
                    tokio::spawn(async move {
                        if let Ok(stream) = TcpStream::connect(addr).await {
                            info!("Node {} reconnected to {}", node_clone.id, peer_id);
                            if let Err(e) = node_clone.handle_outbound(peer_id, stream).await {
                                error!("Reconnection to {} failed: {}", peer_id, e);
                            }
                        }
                    });
                }
            }
        });

        // election + heartbeat + applier
        let tnode = self.clone();
        tokio::spawn(async move { tnode.election_loop().await });

        let hnode = self.clone();
        tokio::spawn(async move { hnode.heartbeat_loop().await });

        let applier = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(100)).await;
                applier.apply_committed_entries().await;
            }
        });

        // client API
        let client_node = self.clone();
        tokio::spawn(async move {
            let client_addr: std::net::SocketAddr = client_node.client_addr.parse().expect("parse client addr");
            if let Err(e) = client_node.run_client_api(client_addr).await {
                error!("client API failed: {:?}", e);
            }
        });

        // delegate listener (followers execute delegated work)
        let delegate_node = self.clone();
        let self_clone = self.clone();
        tokio::spawn(async move {
            let port = 9100 + delegate_node.id;
            let host = self_clone.client_addr.split(':').next().unwrap();
            let addr: std::net::SocketAddr = format!("{}:{}", host, port)
                .parse()
                .expect("parse delegate addr");
            let listener = TcpListener::bind(addr)
                .await
                .expect("delegate bind failed");
            info!("🧩 Node {} delegate listener on {}", delegate_node.id, addr);

            loop {
                match listener.accept().await {
                    Ok((mut stream, _)) => {
                        let mut buf = String::new();
                        let mut reader = BufReader::new(&mut stream);
                        if reader.read_line(&mut buf).await.is_ok() {
                            let cmd = buf.trim();
                            info!("Node {} received delegated command: {}", delegate_node.id, cmd);
                            {
                                let mut p = delegate_node.local_pending.write().await;
                                *p = p.saturating_add(1);
                            }
                            if let Err(e) = delegate_node.execute_delegated(cmd).await {
                                error!("Node {} delegated exec error: {:?}", delegate_node.id, e);
                            }
                            {
                                let mut p = delegate_node.local_pending.write().await;
                                *p = p.saturating_sub(1);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Delegate listener error: {:?}", e);
                    }
                }
            }
        });

        // periodic load reporter
        let report_node = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(1200)).await;
                let pending = *report_node.local_pending.read().await;
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();

                if matches!(*report_node.state.read().await, RaftState::Leader) {
                    let mut t = report_node.load_table.write().await;
                    t.insert(report_node.id, (pending, now_ms));
                    continue;
                }

                if let Some(lid) = *report_node.leader_hint.read().await {
                    let msg = RaftMessage::LoadReport {
                        from_id: report_node.id,
                        pending,
                        ts_millis: now_ms,
                    };
                    report_node.send_message(lid, msg).await;
                }
            }
        });

        // metrics CSV writer (no command; always on)
        let metrics_clone = self.clone();
        tokio::spawn(async move {
            let filename = format!("metrics-node-{}.csv", metrics_clone.id);
            if !std::path::Path::new(&filename).exists() {
                if let Ok(mut f) = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&filename)
                    .await
                {
                    let header = "timestamp,node_id,requests_total,failures_total,elections_won,tasks_executed_local,tasks_executed_delegated,delegated_sent_total,latency_avg_ms\n";
                    let _ = f.write_all(header.as_bytes()).await;
                }
            }

            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let req = metrics_clone.metrics.requests_total.load(Ordering::Relaxed);
                let fail = metrics_clone.metrics.failures_total.load(Ordering::Relaxed);
                let elect = metrics_clone.metrics.elections_won.load(Ordering::Relaxed);
                let local = metrics_clone.metrics.tasks_executed_local.load(Ordering::Relaxed);
                let delegated = metrics_clone.metrics.tasks_executed_delegated.load(Ordering::Relaxed);
                let sent = metrics_clone.metrics.delegated_sent_total.load(Ordering::Relaxed);
                let lat_sum = metrics_clone.metrics.latency_sum_ns.load(Ordering::Relaxed);
                let lat_cnt = metrics_clone.metrics.latency_count.load(Ordering::Relaxed);
                let avg_ms = if lat_cnt > 0 {
                    (lat_sum as f64 / lat_cnt as f64) / 1_000_000.0
                } else {
                    0.0
                };

                let line = format!(
                    "{},{},{},{},{},{},{},{},{}\n",
                    ts,
                    metrics_clone.id,
                    req,
                    fail,
                    elect,
                    local,
                    delegated,
                    sent,
                    avg_ms
                );

                if let Ok(mut f) = OpenOptions::new().append(true).open(&filename).await {
                    let _ = f.write_all(line.as_bytes()).await;
                }
            }
        });

        Ok(())
    }

    async fn handle_inbound(&self, stream: TcpStream) -> anyhow::Result<()> {
        let mut framed = FramedRead::new(stream, LengthDelimitedCodec::new());
        while let Some(frame_res) = framed.next().await {
            let frame = frame_res?;
            let vec = frame.to_vec();
            if let Ok(msg) = serde_json::from_slice::<RaftMessage>(&vec) {
                self.handle_message(msg).await;
            }
        }
        Ok(())
    }

    async fn handle_outbound(&self, peer_id: u32, stream: TcpStream) -> anyhow::Result<()> {
        let (_r, w) = stream.into_split();
        let mut writer = FramedWrite::new(w, LengthDelimitedCodec::new());
        let (tx, mut rx) = mpsc::unbounded_channel::<RaftMessage>();
        {
            let mut out = self.outbound.lock().await;
            out.insert(peer_id, tx.clone());
        }

        let result = async {
            while let Some(msg) = rx.recv().await {
                let payload = serde_json::to_vec(&msg)?;
                if let Err(e) = writer.send(Bytes::from(payload)).await {
                    return Err::<(), anyhow::Error>(e.into());
                }
            }
            Ok(())
        }
        .await;

        {
            let mut out = self.outbound.lock().await;
            out.remove(&peer_id);
        }

        result
    }

    async fn send_message(&self, target: u32, msg: RaftMessage) {
        let out = self.outbound.lock().await;
        if let Some(tx) = out.get(&target) {
            let _ = tx.send(msg);
        } else {
            debug!("Node {}: no outbound for {}", self.id, target);
        }
    }

    async fn handle_message(&self, message: RaftMessage) {
        match message {
            RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                info!("Node {} received vote request from {} for term {}", self.id, candidate_id, term);

                let log = self.log.read().await;
                let our_last_index = log.len() as u64;
                let our_last_term = log.last().map_or(0, |e| e.term);

                let log_is_ok = match our_last_term.cmp(&last_log_term) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => last_log_index >= our_last_index,
                    std::cmp::Ordering::Greater => false,
                };

                if !log_is_ok {
                    info!("Node {} rejecting vote for {} (log out of date)", self.id, candidate_id);
                    let reply = RaftMessage::VoteReply {
                        term: *self.current_term.read().await,
                        vote_granted: false,
                        sender_id: self.id
                    };
                    self.send_message(candidate_id, reply).await;
                    return;
                }

                let mut current_term = self.current_term.write().await;
                let mut voted_for = self.voted_for.write().await;
                if term > *current_term {
                    *current_term = term;
                    *voted_for = None;
                    *self.state.write().await = RaftState::Follower;
                }
                let grant = if term == *current_term {
                    match *voted_for {
                        None => {
                            *voted_for = Some(candidate_id);
                            *self.last_heartbeat.write().await = Instant::now();
                            true
                        }
                        Some(id) => id == candidate_id,
                    }
                } else { false };

                let reply = RaftMessage::VoteReply { term: *current_term, vote_granted: grant, sender_id: self.id };
                self.send_message(candidate_id, reply).await;
            }
            RaftMessage::VoteReply { term, vote_granted, sender_id } => {
                info!("Node {} received vote reply from {} (granted: {})", self.id, sender_id, vote_granted);
                self.handle_vote_reply(term, vote_granted, sender_id).await;
            }
            RaftMessage::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                info!("Node {} received AppendEntries from {} (term {}, {} entries)",
                      self.id, leader_id, term, entries.len());

                {
                    let mut hint = self.leader_hint.write().await;
                    *hint = Some(leader_id);
                }

                let current_term = *self.current_term.read().await;
                let mut success = false;

                if term < current_term {
                    info!("Node {} rejecting AppendEntries (term {} < {})", self.id, term, current_term);
                } else {
                    if term > current_term {
                        *self.current_term.write().await = term;
                        *self.state.write().await = RaftState::Follower;
                        *self.voted_for.write().await = None;
                    }

                    *self.last_heartbeat.write().await = Instant::now();

                    let mut log = self.log.write().await;

                    let log_ok = if prev_log_index == 0 {
                        true
                    } else if prev_log_index > log.len() as u64 {
                        false
                    } else {
                        log.get(prev_log_index as usize - 1)
                           .map_or(false, |e| e.term == prev_log_term)
                    };

                    if !log_ok {
                        info!("Node {} log inconsistency at index {}", self.id, prev_log_index);
                    } else {
                        if prev_log_index < log.len() as u64 {
                            log.truncate(prev_log_index as usize);
                        }
                        log.extend_from_slice(&entries);

                        let mut commit_index = self.commit_index.write().await;
                        if leader_commit > *commit_index {
                            *commit_index = leader_commit.min(log.len() as u64);
                            info!("Node {} updated commit_index to {}", self.id, *commit_index);
                        }
                        drop(commit_index);
                        drop(log);
                        self.apply_committed_entries().await;
                        success = true;
                    }
                }

                let reply = RaftMessage::AppendReply {
                    term: *self.current_term.read().await,
                    sender_id: self.id,
                    success,
                };
                self.send_message(leader_id, reply).await;
            }
            RaftMessage::AppendReply { term, sender_id, success } => {
                info!("Node {} received AppendReply from {} (success: {})", self.id, sender_id, success);

                let current_term = *self.current_term.read().await;
                if term > current_term {
                    *self.current_term.write().await = term;
                    *self.state.write().await = RaftState::Follower;
                    *self.voted_for.write().await = None;
                    return;
                }

                if matches!(*self.state.read().await, RaftState::Leader) {
                    if success {
                        let last_log_index = self.log.read().await.len() as u64;
                        self.update_indices(sender_id, last_log_index).await;
                        info!("Node {} updated indices for {} to {}", self.id, sender_id, last_log_index);
                    } else {
                        let mut next_indices = self.next_index.write().await;
                        if let Some(next_idx) = next_indices.get_mut(&sender_id) {
                            *next_idx = Self::clamp1(next_idx.saturating_sub(1));
                            info!("Node {} decreased next_index for {} to {}", self.id, sender_id, *next_idx);
                        }
                    }
                }
            }
            RaftMessage::LoadReport { from_id, pending, ts_millis } => {
                if matches!(*self.state.read().await, RaftState::Leader) {
                    let mut t = self.load_table.write().await;
                    t.insert(from_id, (pending, ts_millis));
                    let my_pending = *self.local_pending.read().await;
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    t.insert(self.id, (my_pending, now_ms));
                    debug!("Leader {} updated load_table from {} => pending={}", self.id, from_id, pending);
                }
            }
        }
    }

    async fn handle_vote_reply(&self, term: u64, vote_granted: bool, _sender_id: u32) {
        let current_term = *self.current_term.read().await;
        if term > current_term {
            *self.current_term.write().await = term;
            *self.voted_for.write().await = None;
            *self.state.write().await = RaftState::Follower;
            return;
        }
        if matches!(*self.state.read().await, RaftState::Candidate) && term == current_term {
            let mut votes = self.votes_received.write().await;
            let entry = votes.entry(term).or_insert_with(HashMap::new);
            entry.insert(_sender_id, vote_granted);
            let granted_votes = entry.values().filter(|&&v| v).count();
            let total = self.peers.len() + 1;
            info!("Node {}: term {} has {} granted votes out of {}", self.id, term, granted_votes, total);
            if granted_votes > total / 2 && matches!(*self.state.read().await, RaftState::Candidate) {
                *self.state.write().await = RaftState::Leader;
                self.metrics.elections_won.fetch_add(1, Ordering::Relaxed);
                info!("Node {} became leader for term {}", self.id, current_term);

                self.rebuild_state_from_log().await;

                let last_index = self.log.read().await.len() as u64;
                {
                    let mut next_indices = self.next_index.write().await;
                    let mut match_indices = self.match_index.write().await;
                    for (&peer_id, _) in self.peers.iter() {
                        next_indices.insert(peer_id, last_index + 1);
                        match_indices.insert(peer_id, 0);
                    }
                }

                for (&peer_id, _) in self.peers.iter() {
                    let hb = RaftMessage::AppendEntries {
                        term: current_term,
                        leader_id: self.id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit: 0
                    };
                    self.send_message(peer_id, hb).await;
                }
            }
        }
    }

    async fn election_loop(&self) {
        loop {
            let timeout = Duration::from_millis(2000 + rand::random::<u64>() % 2000);
            match *self.state.read().await {
                RaftState::Leader => {
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
                RaftState::Candidate | RaftState::Follower => {
                    let last = *self.last_heartbeat.read().await;
                    if last.elapsed() < timeout {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    info!(
                        "Node {} election timeout after {:?}, starting election",
                        self.id, last.elapsed()
                    );
                }
            }

            if self.failure_simulated().await {
                continue;
            }

            *self.state.write().await = RaftState::Candidate;
            *self.current_term.write().await += 1;
            *self.voted_for.write().await = Some(self.id);
            *self.last_heartbeat.write().await = Instant::now();
            let term = *self.current_term.read().await;
            info!("Node {} starting election for term {}", self.id, term);

            {
                let mut votes = self.votes_received.write().await;
                votes.insert(term, HashMap::new());
                votes.get_mut(&term).unwrap().insert(self.id, true);
            }

            let (last_log_index, last_log_term) = {
                let lg = self.log.read().await;
                let idx = lg.len() as u64;
                let term_of_last = lg.last().map_or(0, |e| e.term);
                (idx, term_of_last)
            };

            let req = RaftMessage::RequestVote {
                term,
                candidate_id: self.id,
                last_log_index,
                last_log_term,
            };

            for (&peer_id, _) in self.peers.iter() {
                self.send_message(peer_id, req.clone()).await;
            }
        }
    }

    async fn failure_simulated(&self) -> bool {
        false
    }

    async fn pick_delegate(&self) -> Option<u32> {
        let mut rng = rand::thread_rng();
        self.peers
            .keys()
            .copied()
            .filter(|&id| id != self.id)
            .choose(&mut rng)
    }

    async fn delegate_execute(&self, delegate_id: u32, command: &str) {
        self.metrics.tasks_executed_delegated.fetch_add(1, Ordering::Relaxed);
        self.metrics.delegated_sent_total.fetch_add(1, Ordering::Relaxed);
        if let Some(peer_sock) = self.peers.get(&delegate_id) {
            let peer_host = peer_sock.ip();
            let port = 9100 + delegate_id;
            let target_addr = format!("{}:{}", peer_host, port);

            match TcpStream::connect(&target_addr).await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(format!("{}\n", command).as_bytes()).await {
                        error!("Leader {} failed to send command to delegate {}: {:?}", self.id, delegate_id, e);
                    } else {
                        info!("Leader {} delegated '{}' to follower {} ({})", self.id, command, delegate_id, target_addr);
                    }
                }
                Err(e) => {
                    error!("Leader {} failed to connect to delegate {} at {}: {:?}", self.id, delegate_id, target_addr, e);
                }
            }
        } else {
            error!("Leader {}: delegate id {} not found in peers", self.id, delegate_id);
        }
    }

    async fn rebuild_state_from_log(&self) {
        let log = self.log.read().await;
        let commit_index = *self.commit_index.read().await;
        info!("Node {} rebuilding state from {} committed entries", self.id, commit_index);

        self.registered_users.write().await.clear();

        for i in 0..commit_index {
            if let Some(entry) = log.get(i as usize) {
                let mut parts = entry.command.split_whitespace();
                match parts.next() {
                    Some("REGISTER") => {
                        if let (Some(user), Some(ip)) = (parts.next(), parts.next()) {
                            self.registered_users
                                .write().await
                                .insert(user.to_string(), ip.to_string());
                        }
                    }
                    Some("UNREGISTER") => {
                        if let Some(user) = parts.next() {
                            self.registered_users.write().await.remove(user);
                        }
                    }
                    _ => {}
                }
            }
        }
        info!("Node {} rebuild complete: {} users restored",
            self.id, self.registered_users.read().await.len());
    }

    async fn apply_committed_entries(&self) {
        let commit_index = *self.commit_index.read().await;
        let mut last_applied = self.last_applied.write().await;

        if commit_index > *last_applied {
            let log = self.log.read().await;
            for i in (*last_applied + 1)..=commit_index {
                if let Some(entry) = log.get(i as usize - 1) {
                    if matches!(*self.state.read().await, RaftState::Leader) {
                        if let Some(delegate_id) = self.pick_delegate().await {
                            self.delegate_execute(delegate_id, &entry.command).await;
                        } else {
                            info!("Node {}: no delegate available, executing locally", self.id);
                            self.execute_locally(&entry.command).await;
                        }
                    }
                    let mut parts = entry.command.split_whitespace();
                    match parts.next() {
                        Some("REGISTER") => {
                            if let (Some(user), Some(ip)) = (parts.next(), parts.next()) {
                                self.registered_users
                                    .write().await
                                    .insert(user.to_string(), ip.to_string());
                                info!("Node {}: Applied REGISTER {} {}", self.id, user, ip);
                            } else {
                                info!("Node {}: Malformed REGISTER command '{}'", self.id, entry.command);
                            }
                        }
                        Some("UNREGISTER") => {
                            if let Some(user) = parts.next() {
                                self.registered_users
                                    .write().await
                                    .remove(user);
                                info!("Node {}: Applied UNREGISTER {}", self.id, user);
                            } else {
                                info!("Node {}: Malformed UNREGISTER command '{}'", self.id, entry.command);
                            }
                        }
                        Some("ENCRYPT_IMAGE") => {
                            if let (Some(_id), Some(passphrase), Some(input_path), Some(output_path)) =
                                (parts.next(), parts.next(), parts.next(), parts.next())
                            {
                                match tokio::fs::read(&input_path).await {
                                    Ok(plaintext_bytes) => {
                                        let cover_path = "images/cover_image.PNG";
                                        match tokio::fs::read(cover_path).await {
                                            Ok(cover_bytes) => {
                                                match encrypt_and_embed_to_png(
                                                    passphrase.as_bytes(),
                                                    &plaintext_bytes,
                                                    &cover_bytes,
                                                ) {
                                                    Ok((stego_bytes, sha, count)) => {
                                                        if let Some(parent) = Path::new(&output_path).parent() {
                                                            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                                                                error!("Node {}: create_dir_all {:?} failed: {:?}", self.id, parent, e);
                                                                continue;
                                                            }
                                                        }
                                                        // Skipping actual file save (as per your previous note)
                                                        info!(
                                                            "Node {}: ENCRYPT_IMAGE computed successfully ({} bytes embedded, sha256={}), skipping file save",
                                                            self.id, count, sha
                                                        );
                                                    }
                                                    Err(e) => error!("Node {}: encryption/embed failed: {:?}", self.id, e),
                                                }
                                            }
                                            Err(e) => error!("Node {}: failed to read cover image {}: {:?}", self.id, cover_path, e),
                                        }
                                    }
                                    Err(e) => error!("Node {}: failed to read {}: {:?}", self.id, input_path, e),
                                }
                            } else {
                                error!("Node {}: malformed ENCRYPT_IMAGE command '{}'", self.id, entry.command);
                            }
                        }
                        Some(other) => {
                            info!("Node {}: Unknown command '{}'", self.id, other);
                        }
                        None => {}
                    }
                }
            }
            *last_applied = commit_index;
        }
    }

    // ---- Local executor (increments task counters) ----
    async fn execute_locally(&self, cmd: &str) {
        self.metrics.tasks_executed_local.fetch_add(1, Ordering::Relaxed);
        let mut parts = cmd.split_whitespace();
        match parts.next() {
            Some("REGISTER") => {
                if let (Some(user), Some(ip)) = (parts.next(), parts.next()) {
                    self.registered_users
                        .write()
                        .await
                        .insert(user.to_string(), ip.to_string());
                    info!("Node {} executed REGISTER {} {}", self.id, user, ip);
                } else {
                    error!("Node {} malformed REGISTER in execute_locally: '{}'", self.id, cmd);
                }
            }
            Some("UNREGISTER") => {
                if let Some(user) = parts.next() {
                    self.registered_users.write().await.remove(user);
                    info!("Node {} executed UNREGISTER {}", self.id, user);
                } else {
                    error!("Node {} malformed UNREGISTER in execute_locally: '{}'", self.id, cmd);
                }
            }
            Some("ENCRYPT_IMAGE") => {
                let args: Vec<_> = parts.collect();
                if args.len() == 4 {
                    let (_id, pass, inp, out) = (args[0], args[1], args[2], args[3]);
                    info!("Node {} executing ENCRYPT_IMAGE {} -> {}", self.id, inp, out);
                    // (logic handled in apply path above)
                } else {
                    error!("Malformed ENCRYPT_IMAGE command: '{}'", cmd);
                }
            }
            Some(other) => info!("Node {}: Unknown command '{}'", self.id, other),
            None => {}
        }
    }

    async fn heartbeat_loop(&self) {
        loop {
            sleep(Duration::from_millis(500)).await;
            if !matches!(*self.state.read().await, RaftState::Leader) {
                continue;
            }
            let term = *self.current_term.read().await;
            let commit_index = *self.commit_index.read().await;
            let log = self.log.read().await;

            self.apply_committed_entries().await;

            for (&peer_id, _) in self.peers.iter() {
                let next_index = {
                    let next_indices = self.next_index.read().await;
                    next_indices.get(&peer_id).copied().unwrap_or(1)
                };
                let (prev_log_index, prev_log_term) = Self::prev_ptr(next_index, &log);
                let start_idx = next_index.saturating_sub(1);
                let start_usize = usize::try_from(start_idx).unwrap_or(0);
                let entries: Vec<LogEntry> = if next_index == 0 || start_usize > log.len() {
                    Vec::new()
                } else {
                    log[start_usize..].to_vec()
                };

                let msg = RaftMessage::AppendEntries {
                    term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: commit_index,
                };
                self.send_message(peer_id, msg).await;
            }
        }
    }

    async fn update_indices(&self, peer_id: u32, acked_index: u64) {
        {
            let mut next_indices = self.next_index.write().await;
            let mut match_indices = self.match_index.write().await;
            match_indices.insert(peer_id, acked_index);
            next_indices.insert(peer_id, acked_index + 1);
        }

        let mut all_matches: Vec<u64> = {
            let m = self.match_index.read().await;
            m.values().copied().collect()
        };
        let leader_last = self.log.read().await.len() as u64;
        all_matches.push(leader_last);
        all_matches.sort_unstable();

        let majority_idx = all_matches[all_matches.len() / 2];
        if majority_idx == 0 {
            return;
        }

        let entry_term = {
            let log = self.log.read().await;
            log.get((majority_idx - 1) as usize).map(|e| e.term)
        };

        if let Some(t) = entry_term {
            let current_term = *self.current_term.read().await;
            if t == current_term {
                let mut commit_index = self.commit_index.write().await;
                if majority_idx > *commit_index {
                    *commit_index = majority_idx;
                    println!("🟢 Node {} advanced commit_index to {}", self.id, majority_idx);
                }
            }
        }
    }

    pub async fn run_client_api(self: Arc<Self>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("🌐 Node {} client API listening on {}", self.id, addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            info!("📡 Node {} accepted client connection from {}", self.id, peer_addr);
            let node = self.clone();
            tokio::spawn(async move {
                if let Err(e) = node.handle_client_connection(stream).await {
                    error!("client connection error: {:?}", e);
                }
            });
        }
    }

    async fn handle_client_connection(&self, mut stream: TcpStream) -> anyhow::Result<()> {
        let (r, mut w) = stream.split();
        let mut reader = BufReader::new(r);
        let mut line = String::new();

        w.write_all(b"Welcome to Cloud P2P Node API!\n").await?;
        w.write_all(b"Commands: LEADER | REGISTER <user> <ip> | UNREGISTER <user> | LIST | SHOW_USERS | SUBMIT <op_id> <command...> | ENCRYPT_IMAGE <id> <passphrase> <input_path> <output_path>\n").await?;

        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }

            let start = self.metrics_request_start();
            let cmd = line.trim();
            let is_leader = matches!(*self.state.read().await, RaftState::Leader);
            let mut parts = cmd.split_whitespace();

            match parts.next() {
                Some("LEADER") => {
                    if is_leader {
                        w.write_all(format!("LEADER {}\n", self.id).as_bytes()).await?;
                    } else if let Some(lid) = *self.leader_hint.read().await {
                        let addr = self.client_addr_for(lid);
                        w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                    } else {
                        w.write_all(b"NOT_LEADER\n").await?;
                    }
                    self.metrics_ok(start);
                }

                Some("REGISTER") => {
                    if !is_leader {
                        match self.forward_to_leader(cmd).await {
                            Ok(reply) => {
                                w.write_all(reply.as_bytes()).await?;
                                self.metrics_ok(start);
                            }
                            Err(_) => {
                                if let Some(lid) = *self.leader_hint.read().await {
                                    let addr = self.client_addr_for(lid);
                                    w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                                    self.metrics_ok(start);
                                } else {
                                    w.write_all(b"NOT_LEADER\n").await?;
                                    self.metrics_fail(start);
                                }
                            }
                        }
                        continue;
                    }
                    let user = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: REGISTER <user> <ip>\n").await?;
                            self.metrics_fail(start);
                            continue;
                        }
                    };
                    let ip = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: REGISTER <user> <ip>\n").await?;
                            self.metrics_fail(start);
                            continue;
                        }
                    };

                    let command = format!("REGISTER {} {}", user, ip);
                    self.append_and_replicate(command).await;
                    w.write_all(b"OK\n").await?;
                    self.metrics_ok(start);
                }

                Some("UNREGISTER") => {
                    if !is_leader {
                        match self.forward_to_leader(cmd).await {
                            Ok(reply) => {
                                w.write_all(reply.as_bytes()).await?;
                                self.metrics_ok(start);
                            }
                            Err(_) => {
                                if let Some(lid) = *self.leader_hint.read().await {
                                    let addr = self.client_addr_for(lid);
                                    w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                                    self.metrics_ok(start);
                                } else {
                                    w.write_all(b"NOT_LEADER\n").await?;
                                    self.metrics_fail(start);
                                }
                            }
                        }
                        continue;
                    }
                    let user = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: UNREGISTER <user>\n").await?;
                            self.metrics_fail(start);
                            continue;
                        }
                    };
                    let command = format!("UNREGISTER {}", user);
                    self.append_and_replicate(command).await;
                    w.write_all(b"OK\n").await?;
                    self.metrics_ok(start);
                }

                Some("LIST") => {
                    let log = self.log.read().await;
                    if log.is_empty() {
                        w.write_all(b"(empty)\n").await?;
                    } else {
                        for e in log.iter() {
                            let s = format!("idx={} term={} cmd={}\n", e.index, e.term, e.command);
                            w.write_all(s.as_bytes()).await?;
                        }
                    }
                    self.metrics_ok(start);
                }

                Some("SHOW_USERS") => {
                    if !is_leader {
                        match self.forward_to_leader("SHOW_USERS").await {
                            Ok(reply) => {
                                w.write_all(reply.as_bytes()).await?;
                                self.metrics_ok(start);
                            }
                            Err(_) => {
                                if let Some(lid) = *self.leader_hint.read().await {
                                    let addr = self.client_addr_for(lid);
                                    w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                                    self.metrics_ok(start);
                                } else {
                                    w.write_all(b"NOT_LEADER\n").await?;
                                    self.metrics_fail(start);
                                }
                            }
                        }
                        continue;
                    }
                    let users = self.registered_users.read().await;
                    if users.is_empty() {
                        w.write_all(b"(empty)\n").await?;
                    } else {
                        for (u, ip) in users.iter() {
                            w.write_all(format!("{} {}\n", u, ip).as_bytes()).await?;
                        }
                    }
                    self.metrics_ok(start);
                }

                Some("SUBMIT") => {
                    let op_id = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: SUBMIT <op_id> <command...>\n").await?;
                            self.metrics_fail(start);
                            continue;
                        }
                    };
                    let rest = parts.collect::<Vec<_>>().join(" ");
                    if rest.is_empty() {
                        w.write_all(b"Usage: SUBMIT <op_id> <command...>\n").await?;
                        self.metrics_fail(start);
                        continue;
                    }

                    {
                        let seen = self.processed_ops.read().await;
                        if seen.contains(&op_id) {
                            w.write_all(b"OK\n").await?;
                            self.metrics_ok(start);
                            continue;
                        }
                    }

                    if !is_leader {
                        if let Some(lid) = *self.leader_hint.read().await {
                            let addr = self.client_addr_for(lid);
                            w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                            self.metrics_ok(start);
                        } else {
                            w.write_all(b"NOT_LEADER\n").await?;
                            self.metrics_fail(start);
                        }
                        continue;
                    }

                    {
                        let mut seen = self.processed_ops.write().await;
                        seen.insert(op_id);
                    }
                    self.append_and_replicate(rest).await;
                    w.write_all(b"OK\n").await?;
                    self.metrics_ok(start);
                }

                Some("ENCRYPT_IMAGE") => {
                    if !is_leader {
                        if let Some(lid) = *self.leader_hint.read().await {
                            let addr = self.client_addr_for(lid);
                            w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                            self.metrics_ok(start);
                        } else {
                            w.write_all(b"NOT_LEADER\n").await?;
                            self.metrics_fail(start);
                        }
                        continue;
                    }

                    let id = parts.next().unwrap_or_default().to_string();
                    let passphrase = parts.next().unwrap_or_default().to_string();
                    let input_path = parts.next().unwrap_or_default().to_string();
                    let output_path = parts.next().unwrap_or_default().to_string();

                    if id.is_empty() || passphrase.is_empty() || input_path.is_empty() || output_path.is_empty() {
                        w.write_all(b"Usage: ENCRYPT_IMAGE <id> <passphrase> <input_path> <output_path>\n").await?;
                        self.metrics_fail(start);
                        continue;
                    }

                    let command = format!("ENCRYPT_IMAGE {} {} {} {}", id, passphrase, input_path, output_path);
                    self.append_and_replicate(command).await;
                    w.write_all(b"OK\n").await?;
                    self.metrics_ok(start);
                }

                Some(unknown) => {
                    let s = format!("ERR unknown command: {}\n", unknown);
                    w.write_all(s.as_bytes()).await?;
                    self.metrics_fail(start);
                }

                None => {
                    // empty line; treat as no-op, do not mark failure
                    self.metrics_ok(start);
                }
            }
        }
        Ok(())
    }

    async fn append_and_replicate(&self, command: String) {
        if !matches!(*self.state.read().await, RaftState::Leader) {
            info!("Node {}: append ignored (not leader)", self.id);
            return;
        }

        let term = *self.current_term.read().await;
        let index = {
            let mut log = self.log.write().await;
            let index = (log.len() as u64) + 1;
            log.push(LogEntry { term, index, command: command.clone() });
            index
        };
        info!("Node {}: appended new entry #{} '{}'", self.id, index, command);

        let term_now = *self.current_term.read().await;
        let commit_index = *self.commit_index.read().await;
        let log_snapshot = self.log.read().await;

        for (&peer_id, _) in self.peers.iter() {
            let next_index = {
                let next_indices = self.next_index.read().await;
                next_indices.get(&peer_id).copied().unwrap_or(1)
            };

            let (prev_log_index, prev_log_term) = Self::prev_ptr(next_index, &log_snapshot);
            let start_idx = next_index.saturating_sub(1);
            let start_usize = usize::try_from(start_idx).unwrap_or(0);
            let entries = if start_usize > log_snapshot.len() {
                Vec::new()
            } else {
                log_snapshot[start_usize..].to_vec()
            };

            let msg = RaftMessage::AppendEntries {
                term: term_now,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };
            self.send_message(peer_id, msg).await;
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let mut peers_map = HashMap::new();
    for pair in args.peers.split(',') {
        if pair.trim().is_empty() { continue; }
        if let Some((id_s, addr)) = pair.split_once('=') {
            let id: u32 = id_s.parse()?;
            let sa: SocketAddr = addr.parse()?;
            peers_map.insert(id, sa);
        }
    }
    let listen: SocketAddr = args.addr.parse()?;
    let node = Arc::new(NetNode::new(args.id, peers_map, args.client_addr.clone()));
    node.clone().start(listen).await?;
    loop { sleep(Duration::from_secs(3600)).await; }
}