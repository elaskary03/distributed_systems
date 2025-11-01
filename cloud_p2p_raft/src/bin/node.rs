use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::codec::{LengthDelimitedCodec, FramedRead, FramedWrite};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tracing::{debug, error, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use rand::random;

/// ─────────────────────────────────────────────────────────────────────────────
/// CLI
/// ─────────────────────────────────────────────────────────────────────────────
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Node id (unique per node)
    #[arg(long)]
    id: u32,

    /// Listen address for node-to-node RPC (e.g. "127.0.0.1:7001")
    #[arg(long)]
    addr: String,

    /// Peers list: comma-separated id=addr (e.g. "2=127.0.0.1:7002,3=127.0.0.1:7003")
    #[arg(long)]
    peers: String,

    /// Client API listen address (default auto-maps to 9000+id)
    #[arg(long, default_value = "127.0.0.1:9000")]
    client_addr: String,
}

/// ─────────────────────────────────────────────────────────────────────────────
/// Raft data types
/// ─────────────────────────────────────────────────────────────────────────────
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
}

/// ─────────────────────────────────────────────────────────────────────────────
/// NetNode
/// ─────────────────────────────────────────────────────────────────────────────
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

    // application state
    kv: Arc<RwLock<HashMap<String, String>>>,
    processed_ops: Arc<RwLock<HashSet<String>>>,
    registered_users: Arc<RwLock<HashMap<String, String>>>, // user -> ip

    // Quiescent-mode controls
    election_armed: Arc<RwLock<bool>>,          // false until a client request arrives
    last_client_activity: Arc<RwLock<Instant>>, // refreshed by client requests & leader writes
    inactivity_timeout: Duration,               // step-down window (default 10s)
}

impl NetNode {
    fn new(id: u32, peers: HashMap<u32, SocketAddr>) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for (&pid, _) in peers.iter() {
            next_index.insert(pid, 1);
            match_index.insert(pid, 0);
        }

        Self {
            id,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),

            peers: Arc::new(peers),
            outbound: Arc::new(Mutex::new(HashMap::new())),

            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            votes_received: Arc::new(RwLock::new(HashMap::new())),

            log: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            next_index: Arc::new(RwLock::new(next_index)),
            match_index: Arc::new(RwLock::new(match_index)),

            leader_hint: Arc::new(RwLock::new(None)),

            kv: Arc::new(RwLock::new(HashMap::new())),
            processed_ops: Arc::new(RwLock::new(HashSet::new())),
            registered_users: Arc::new(RwLock::new(HashMap::new())),

            election_armed: Arc::new(RwLock::new(false)),
            last_client_activity: Arc::new(RwLock::new(Instant::now())),
            inactivity_timeout: Duration::from_secs(10),
        }
    }

    #[inline]
    fn prev_ptr(next_index: u64, log: &[LogEntry]) -> (u64, u64) {
        if next_index <= 1 {
            return (0, 0);
        }
        let prev = (next_index - 1) as usize;
        let prev_term = log.get(prev - 1).map_or(0, |e| e.term);
        (prev as u64, prev_term)
    }

    /// Quiet writes to client sockets (suppress common disconnect errors)
    async fn write_quiet(
        w: &mut tokio::net::tcp::OwnedWriteHalf,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        use std::io::ErrorKind;
        match w.write_all(bytes).await {
            Ok(_) => Ok(()),
            Err(e) if matches!(
                e.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
            ) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// ─────────────────────────────────────────────────────────────────────────
    /// Startup
    /// ─────────────────────────────────────────────────────────────────────────
    async fn start(self: Arc<Self>, listen_addr: SocketAddr, client_addr: SocketAddr) -> Result<()> {
        // Node-to-node RPC listener
        let server = TcpListener::bind(listen_addr).await?;
        info!("Node {} RPC listening on {}", self.id, listen_addr);

        // Accept loop for node-to-node
        {
            let me = self.clone();
            tokio::spawn(async move {
                loop {
                    match server.accept().await {
                        Ok((stream, addr)) => {
                            let me2 = me.clone();
                            tokio::spawn(async move {
                                if let Err(e) = me2.handle_inbound(stream, addr).await {
                                    error!("[{}] inbound error: {}", me2.id, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("[{}] accept error: {}", me.id, e);
                            sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }

        // Outbound connectors (retry)
        {
            let me = self.clone();
            tokio::spawn(async move {
                loop {
                    for (&peer_id, &peer_sa) in me.peers.iter() {
                        if peer_id == me.id { continue; }
                        let already = me.outbound.lock().await.contains_key(&peer_id);
                        if already { continue; }

                        match TcpStream::connect(peer_sa).await {
                            Ok(stream) => {
                                info!("[{}] connected to peer {} @ {}", me.id, peer_id, peer_sa);
                                let me2 = me.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = me2.handle_outbound(peer_id, stream).await {
                                        error!("[{}] outbound {} error: {}", me2.id, peer_id, e);
                                    }
                                });
                            }
                            Err(_) => {}
                        }
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            });
        }

        // Election loop (only when armed)
        {
            let me = self.clone();
            tokio::spawn(async move {
                me.election_loop().await;
            });
        }

        // Heartbeat loop (leader only)
        {
            let me = self.clone();
            tokio::spawn(async move {
                me.heartbeat_loop().await;
            });
        }

        // Apply loop
        {
            let me = self.clone();
            tokio::spawn(async move {
                me.apply_committed_entries_loop().await;
            });
        }

        // Inactivity step-down loop
        {
            let me = self.clone();
            tokio::spawn(async move {
                me.inactivity_loop().await;
            });
        }

        // Client API server
        {
            let my_id = self.id;
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.clone().run_client_api(client_addr).await {
                    debug!("[{}] client API ended: {}", my_id, e);
                }
            });
        }

        Ok(())
    }

    /// ─────────────────────────────────────────────────────────────────────────
    /// Node-to-node RPC plumbing
    /// ─────────────────────────────────────────────────────────────────────────
    async fn handle_inbound(&self, stream: TcpStream, _addr: SocketAddr) -> Result<()> {
        let (r, _w) = stream.into_split();
        let mut reader = FramedRead::new(r, LengthDelimitedCodec::new());
        while let Some(Ok(bytes)) = reader.next().await {
            match serde_json::from_slice::<RaftMessage>(&bytes) {
                Ok(msg) => self.handle_message(msg).await,
                Err(e) => error!("[{}] decode inbound error: {}", self.id, e),
            }
        }
        Ok(())
    }

    async fn handle_outbound(&self, peer_id: u32, stream: TcpStream) -> Result<()> {
        let (r, w) = stream.into_split();
        let mut reader = FramedRead::new(r, LengthDelimitedCodec::new());
        let writer = FramedWrite::new(w, LengthDelimitedCodec::new());

        // register sender channel
        let (tx, mut rx) = mpsc::unbounded_channel::<RaftMessage>();
        self.outbound.lock().await.insert(peer_id, tx);

        // forward loop
        let my_id = self.id;
        let mut writer_owned = writer;
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match serde_json::to_vec(&msg) {
                    Ok(buf) => {
                        if let Err(e) = writer_owned.send(Bytes::from(buf)).await {
                            error!("[{}] forward->peer {} send error: {}", my_id, peer_id, e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("[{}] forward->peer {} serialize error: {}", my_id, peer_id, e);
                        break;
                    }
                }
            }
        });

        // read loop
        while let Some(Ok(bytes)) = reader.next().await {
            match serde_json::from_slice::<RaftMessage>(&bytes) {
                Ok(msg) => self.handle_message(msg).await,
                Err(e) => error!("[{}] decode inbound-from-{} error: {}", self.id, peer_id, e),
            }
        }

        self.outbound.lock().await.remove(&peer_id);
        Ok(())
    }

    async fn send_message(&self, target: u32, msg: RaftMessage) {
        if let Some(tx) = self.outbound.lock().await.get(&target).cloned() {
            let _ = tx.send(msg);
        }
    }

    /// ─────────────────────────────────────────────────────────────────────────
    /// Raft handlers
    /// ─────────────────────────────────────────────────────────────────────────
    async fn handle_message(&self, message: RaftMessage) {
        match message {
            RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                let mut vote_granted = false;

                // Candidate's log up-to-dateness
                let (our_last_index, our_last_term) = {
                    let log = self.log.read().await;
                    (log.len() as u64, log.last().map_or(0, |e| e.term))
                };
                let log_is_ok = match our_last_term.cmp(&last_log_term) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => our_last_index <= last_log_index,
                };

                let ct = *self.current_term.read().await;
                if term < ct {
                    vote_granted = false;
                } else {
                    if term > ct {
                        *self.current_term.write().await = term;
                        *self.voted_for.write().await = None;
                        *self.state.write().await = RaftState::Follower;
                    }
                    let voted_for = *self.voted_for.read().await;
                    if (voted_for.is_none() || voted_for == Some(candidate_id)) && log_is_ok {
                        *self.voted_for.write().await = Some(candidate_id);
                        vote_granted = true;
                    }
                }

                let reply = RaftMessage::VoteReply {
                    term: *self.current_term.read().await,
                    vote_granted,
                    sender_id: self.id,
                };
                self.send_message(candidate_id, reply).await;
            }

            RaftMessage::VoteReply { term, vote_granted, sender_id } => {
                self.handle_vote_reply(term, vote_granted, sender_id).await;
            }

            RaftMessage::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                let mut success = false;
                let ct = *self.current_term.read().await;

                if term < ct {
                    success = false;
                } else {
                    if term > ct {
                        *self.current_term.write().await = term;
                        *self.voted_for.write().await = None;
                    }
                    *self.state.write().await = RaftState::Follower;
                    *self.last_heartbeat.write().await = Instant::now();
                    *self.leader_hint.write().await = Some(leader_id);

                    // Log consistency
                    let mut log = self.log.write().await;
                    let log_ok = if prev_log_index == 0 {
                        true
                    } else if (prev_log_index as usize) <= log.len() {
                        log.get((prev_log_index - 1) as usize).map_or(false, |e| e.term == prev_log_term)
                    } else {
                        false
                    };

                    if log_ok {
                        if prev_log_index < log.len() as u64 {
                            log.truncate(prev_log_index as usize);
                        }
                        log.extend_from_slice(&entries);

                        let mut commit_index = self.commit_index.write().await;
                        if leader_commit > *commit_index {
                            *commit_index = leader_commit.min(log.len() as u64);
                        }
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

            RaftMessage::AppendReply { .. } => {
                // No-op in this lightweight demo.
            }
        }
    }

    async fn handle_vote_reply(&self, term: u64, vote_granted: bool, sender_id: u32) {
        let current_term = *self.current_term.read().await;
        if term > current_term {
            *self.current_term.write().await = term;
            *self.voted_for.write().await = None;
            *self.state.write().await = RaftState::Follower;
            return;
        }

        if !matches!(*self.state.read().await, RaftState::Candidate) || term != current_term {
            return;
        }

        let mut votes = self.votes_received.write().await;
        let entry = votes.entry(term).or_insert_with(HashMap::new);
        entry.insert(sender_id, vote_granted);
        let granted = entry.values().filter(|&&v| v).count();
        let total = self.peers.len() + 1;

        if granted * 2 > total {
            *self.state.write().await = RaftState::Leader;
            *self.leader_hint.write().await = Some(self.id);

            // Reset replication pointers
            let log_len = self.log.read().await.len() as u64;
            {
                let mut next_i = self.next_index.write().await;
                let mut match_i = self.match_index.write().await;
                for (&pid, _) in self.peers.iter() {
                    next_i.insert(pid, log_len + 1);
                    match_i.insert(pid, 0);
                }
            }

            // Kick initial heartbeats
            let term_now = *self.current_term.read().await;
            for (&pid, _) in self.peers.iter() {
                let hb = RaftMessage::AppendEntries {
                    term: term_now,
                    leader_id: self.id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: *self.commit_index.read().await,
                };
                self.send_message(pid, hb).await;
            }
        }
    }

    /// ─────────────────────────────────────────────────────────────────────────
    /// Raft loops (quiescent mode)
    /// ─────────────────────────────────────────────────────────────────────────
    async fn begin_election(&self) {
        *self.state.write().await = RaftState::Candidate;
        *self.current_term.write().await += 1;
        let term = *self.current_term.read().await;
        *self.voted_for.write().await = Some(self.id);
        *self.last_heartbeat.write().await = Instant::now();

        {
            let mut votes = self.votes_received.write().await;
            votes.insert(term, HashMap::new());
            if let Some(map) = votes.get_mut(&term) {
                map.insert(self.id, true);
            }
        }

        let (last_log_index, last_log_term) = {
            let log = self.log.read().await;
            (log.len() as u64, log.last().map_or(0, |e| e.term))
        };

        info!("[{}] starting on-demand election for term {}", self.id, term);
        for (&pid, _) in self.peers.iter() {
            let req = RaftMessage::RequestVote {
                term,
                candidate_id: self.id,
                last_log_index,
                last_log_term,
            };
            self.send_message(pid, req).await;
        }
    }

    async fn trigger_election_now_if_needed(&self) {
    {
        let mut armed = self.election_armed.write().await;
        if !*armed {
            *armed = true;
            debug!("[{}] elections armed by client activity", self.id);
        }
    }

    if !matches!(*self.state.read().await, RaftState::Leader) && self.leader_hint.read().await.is_none() {
        // small randomized delay (50–250 ms) so different nodes sometimes win
        let jitter = 50 + (rand::random::<u64>() % 200);
        sleep(Duration::from_millis(jitter)).await;

        // still no leader? start election
        if !matches!(*self.state.read().await, RaftState::Leader) && self.leader_hint.read().await.is_none() {
            self.begin_election().await;
        }
    }
}


    async fn record_client_activity(&self) {
        *self.last_client_activity.write().await = Instant::now();
    }

    async fn inactivity_loop(&self) {
        loop {
            sleep(Duration::from_millis(250)).await;
            if matches!(*self.state.read().await, RaftState::Leader) {
                let last = *self.last_client_activity.read().await;
                if last.elapsed() >= self.inactivity_timeout {
                    info!("[{}] idle for {:?}; stepping down", self.id, self.inactivity_timeout);
                    *self.state.write().await = RaftState::Follower;
                    *self.leader_hint.write().await = None;
                    *self.voted_for.write().await = None;
                    self.votes_received.write().await.clear();
                    *self.election_armed.write().await = false; // disarm
                    *self.last_heartbeat.write().await = Instant::now();
                }
            }
        }
    }

    async fn election_loop(&self) {
        loop {
            if !*self.election_armed.read().await {
                sleep(Duration::from_millis(80)).await;
                continue;
            }

            let timeout = Duration::from_millis(1750 + (random::<u64>() % 1250));

            match *self.state.read().await {
                RaftState::Leader => {
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }
                RaftState::Candidate | RaftState::Follower => {
                    let last = *self.last_heartbeat.read().await;
                    if last.elapsed() < timeout {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }

            self.begin_election().await;
        }
    }

    async fn heartbeat_loop(&self) {
        loop {
            sleep(Duration::from_millis(400)).await;
            if !matches!(*self.state.read().await, RaftState::Leader) {
                continue;
            }

            let term = *self.current_term.read().await;
            let commit_index = *self.commit_index.read().await;
            let log = self.log.read().await;

            for (&pid, _) in self.peers.iter() {
                let next_index = {
                    let map = self.next_index.read().await;
                    map.get(&pid).copied().unwrap_or(1)
                };
                let (prev_log_index, prev_log_term) = Self::prev_ptr(next_index, &log);
                let start_idx = next_index.saturating_sub(1) as usize;
                let entries: Vec<LogEntry> = if start_idx > log.len() { vec![] } else { log[start_idx..].to_vec() };

                let msg = RaftMessage::AppendEntries {
                    term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: commit_index,
                };
                self.send_message(pid, msg).await;
            }
        }
    }

    async fn apply_committed_entries_loop(&self) {
        loop {
            {
                let mut last_applied = self.last_applied.write().await;
                let commit_index = *self.commit_index.read().await;
                while *last_applied < commit_index {
                    *last_applied += 1;
                    let idx = *last_applied as usize - 1;
                    let entry = self.log.read().await.get(idx).cloned();
                    if let Some(e) = entry {
                        self.apply(e).await;
                    }
                }
            }
            sleep(Duration::from_millis(80)).await;
        }
    }

    async fn apply(&self, entry: LogEntry) {
        let parts: Vec<_> = entry.command.split_whitespace().collect();
        if parts.is_empty() { return; }

        match parts[0] {
            "SET" if parts.len() >= 3 => {
                let k = parts[1].to_string();
                let v = parts[2..].join(" ");
                self.kv.write().await.insert(k, v);
            }
            "REGISTER" if parts.len() == 3 => {
                let user = parts[1].to_string();
                let ip   = parts[2].to_string();
                self.registered_users.write().await.insert(user, ip);
            }
            "UNREGISTER" if parts.len() == 2 => {
                let user = parts[1];
                self.registered_users.write().await.remove(user);
            }
            _ => { /* ignore unknown */ }
        }
    }

    /// ─────────────────────────────────────────────────────────────────────────
    /// Client API (GUI/Proxy)
    /// ─────────────────────────────────────────────────────────────────────────
    async fn run_client_api(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Node {} client API listening on {}", self.id, addr);

        loop {
            let (stream, _addr) = listener.accept().await?;
            let my_id = self.id;
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.clone().handle_client_conn(stream).await {
                    debug!("[{}] client conn ended: {}", my_id, e);
                }
            });
        }
    }

    async fn handle_client_conn(self: Arc<Self>, stream: TcpStream) -> anyhow::Result<()> {
        use std::io::ErrorKind;
        use tokio::time::timeout;

        let (mut r, mut w) = stream.into_split();
        let mut buf = Vec::with_capacity(512);

        loop {
            buf.clear();

            let mut tmp = [0u8; 512];
            for _ in 0..8 {
                match timeout(Duration::from_millis(200), r.read(&mut tmp)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => {
                        buf.extend_from_slice(&tmp[..n]);
                        if buf.contains(&b'\n') { break; }
                    }
                    Ok(Err(e)) => {
                        if matches!(e.kind(),
                            ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                            | ErrorKind::BrokenPipe
                            | ErrorKind::UnexpectedEof
                        ) {
                            return Ok(());
                        }
                        return Err(e.into());
                    }
                    Err(_elapsed) => {
                        if !buf.is_empty() { break; }
                    }
                }
            }

            if buf.is_empty() {
                return Ok(());
            }

            let line = if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                &buf[..pos]
            } else {
                &buf[..]
            };
            let cmd = String::from_utf8_lossy(line).trim().to_string();
            if cmd.is_empty() {
                Self::write_quiet(&mut w, b"ERR empty\n").await?;
                continue;
            }

            // Client activity → arm elections / possibly trigger immediately
            self.record_client_activity().await;
            if !matches!(*self.state.read().await, RaftState::Leader) {
                self.trigger_election_now_if_needed().await;
            }

            // Parse commands (supports SUBMIT <op_id> <INNER...>)
            let mut parts = cmd.split_whitespace();
            match parts.next() {
                Some("LEADER") => {
                    if matches!(*self.state.read().await, RaftState::Leader) {
                        Self::write_quiet(&mut w, format!("LEADER {}\n", self.id).as_bytes()).await?;
                    } else if let Some(lid) = *self.leader_hint.read().await {
                        Self::write_quiet(&mut w, format!("REDIRECT {}\n", Self::client_addr_for(lid)).as_bytes()).await?;
                    } else {
                        Self::write_quiet(&mut w, b"NOT_LEADER\n").await?;
                    }
                }

                Some("SHOW_USERS") => {
                    let map = self.registered_users.read().await;
                    if map.is_empty() {
                        Self::write_quiet(&mut w, b"<empty>\n").await?;
                    } else {
                        for (u, ip) in map.iter() {
                            Self::write_quiet(&mut w, format!("{} {}\n", u, ip).as_bytes()).await?;
                        }
                    }
                }

                Some("LIST") => {
                    let committed = *self.commit_index.read().await;
                    Self::write_quiet(&mut w, format!("COMMITTED {}\n", committed).as_bytes()).await?;
                }

                Some("GET") => {
                    let k = match parts.next() { Some(s) => s, None => { Self::write_quiet(&mut w, b"ERR usage GET <k>\n").await?; continue; } };
                    let val = self.kv.read().await.get(k).cloned().unwrap_or_else(|| "<nil>".into());
                    Self::write_quiet(&mut w, format!("{}\n", val).as_bytes()).await?;
                }

                // SUBMIT <op_id> <INNER...>
                Some("SUBMIT") => {
                    let op_id = match parts.next() { Some(x) => x.to_string(), None => { Self::write_quiet(&mut w, b"ERR usage SUBMIT <op_id> <cmd...>\n").await?; continue; } };

                    // If we've already processed this op-id, return OK (idempotent)
                    {
                        let seen = self.processed_ops.read().await;
                        if seen.contains(&op_id) {
                            Self::write_quiet(&mut w, b"OK\n").await?;
                            continue;
                        }
                    }

                    // Determine inner command
                    let inner_first = match parts.next() {
                        Some(x) => x,
                        None => { Self::write_quiet(&mut w, b"ERR usage SUBMIT <op_id> <cmd...>\n").await?; continue; }
                    };

                    // Leader routing
                    if !matches!(*self.state.read().await, RaftState::Leader) {
                        if let Some(lid) = *self.leader_hint.read().await {
                            Self::write_quiet(&mut w, format!("REDIRECT {}\n", Self::client_addr_for(lid)).as_bytes()).await?;
                        } else {
                            Self::write_quiet(&mut w, b"RETRY\n").await?;
                        }
                        continue;
                    }

                    // Build the actual log command based on the inner verb
                    let inner_command = match inner_first {
                        "REGISTER" => {
                            let user = match parts.next() { Some(s) => s, None => { Self::write_quiet(&mut w, b"ERR usage REGISTER <user> <ip>\n").await?; continue; } };
                            let ip   = match parts.next() { Some(s) => s, None => { Self::write_quiet(&mut w, b"ERR usage REGISTER <user> <ip>\n").await?; continue; } };
                            format!("REGISTER {} {}", user, ip)
                        }
                        "UNREGISTER" => {
                            let user = match parts.next() { Some(s) => s, None => { Self::write_quiet(&mut w, b"ERR usage UNREGISTER <user>\n").await?; continue; } };
                            format!("UNREGISTER {}", user)
                        }
                        "SET" => {
                            let k = match parts.next() { Some(s) => s, None => { Self::write_quiet(&mut w, b"ERR usage SET <k> <v>\n").await?; continue; } };
                            let v = parts.collect::<Vec<_>>().join(" ");
                            if v.is_empty() { Self::write_quiet(&mut w, b"ERR usage SET <k> <v>\n").await?; continue; }
                            format!("SET {} {}", k, v)
                        }
                        // Allow unknown inner verbs to be appended as-is (optional)
                        other => {
                            let rest = parts.collect::<Vec<_>>().join(" ");
                            if rest.is_empty() { other.to_string() } else { format!("{} {}", other, rest) }
                        }
                    };

                    // Append & replicate; mark idempotent id as processed
                    self.append_and_replicate(inner_command).await;
                    self.processed_ops.write().await.insert(op_id);
                    Self::write_quiet(&mut w, b"OK\n").await?;
                }

                // Backward-compat direct writes (leader only)
                Some("REGISTER") | Some("UNREGISTER") | Some("SET") => {
                    if !matches!(*self.state.read().await, RaftState::Leader) {
                        if let Some(lid) = *self.leader_hint.read().await {
                            Self::write_quiet(&mut w, format!("REDIRECT {}\n", Self::client_addr_for(lid)).as_bytes()).await?;
                        } else {
                            Self::write_quiet(&mut w, b"RETRY\n").await?;
                        }
                        continue;
                    }

                    let reconstructed = cmd; // already the full line
                    self.append_and_replicate(reconstructed).await;
                    Self::write_quiet(&mut w, b"OK\n").await?;
                }

                Some(_) | None => {
                    Self::write_quiet(&mut w, b"ERR unknown\n").await?;
                }
            }
        }
    }

    async fn append_and_replicate(&self, command: String) {
        if !matches!(*self.state.read().await, RaftState::Leader) {
            return;
        }

        self.record_client_activity().await;

        let term = *self.current_term.read().await;
        let index = {
            let mut log = self.log.write().await;
            let idx = (log.len() as u64) + 1;
            log.push(LogEntry { term, index: idx, command: command.clone() });
            idx
        };
        debug!("[{}] appended #{} '{}'", self.id, index, command);

        // DEMO: mark committed immediately (so LIST shows progress)
        {
            let mut ci = self.commit_index.write().await;
            *ci = (*ci).max(index);
        }

        // Send AppendEntries to followers (best-effort)
        let term_now = *self.current_term.read().await;
        let commit_index = *self.commit_index.read().await;
        let log_snapshot = self.log.read().await;

        for (&peer_id, _) in self.peers.iter() {
            let next_index = {
                let map = self.next_index.read().await;
                map.get(&peer_id).copied().unwrap_or(1)
            };
            let (prev_log_index, prev_log_term) = Self::prev_ptr(next_index, &log_snapshot);
            let start_idx = next_index.saturating_sub(1) as usize;
            let entries: Vec<LogEntry> = if start_idx > log_snapshot.len() {
                vec![]
            } else {
                log_snapshot[start_idx..].to_vec()
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

    fn client_addr_for(id: u32) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9000 + id as u16))
    }
}

/// ─────────────────────────────────────────────────────────────────────────────
/// main
/// ─────────────────────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Parse peers
    let mut peers_map = HashMap::new();
    for pair in args.peers.split(',') {
        let p = pair.trim();
        if p.is_empty() { continue; }
        let (id_s, addr) = match p.split_once('=') {
            Some(x) => x,
            None => {
                error!("Bad peer spec '{}'; expected id=ip:port", p);
                continue;
            }
        };
        let id: u32 = id_s.parse()?;
        let sa: SocketAddr = addr.parse()?;
        peers_map.insert(id, sa);
    }

    let listen_sa: SocketAddr = args.addr.parse()?;

    // Default client port → 9000+id if user kept default 127.0.0.1:9000
    let client_sa: SocketAddr = if args.client_addr == "127.0.0.1:9000" {
        format!("127.0.0.1:{}", 9000 + args.id as u16).parse()?
    } else {
        args.client_addr.parse()?
    };

    let node = Arc::new(NetNode::new(args.id, peers_map));
    node.clone().start(listen_sa, client_sa).await?;

    loop { sleep(Duration::from_secs(3600)).await; }
}
