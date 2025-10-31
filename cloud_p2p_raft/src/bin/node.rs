use base64::Engine; // Import the Engine trait for base64 decoding
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
use futures::{StreamExt, SinkExt};
use tracing::{debug, info, error};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use sha2::{Sha256, Digest};
use hex;
use tokio::io::AsyncReadExt; // for reader.read(...)
use tokio::fs;


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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientRequest {
    /// { "type": "Submit", "command": "your string command" }
    Submit { command: String },
    /// { "type": "GetLeader" }
    GetLeader,
    /// { "type": "GetState" }
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
    // Hint for who is leader (updated when we receive AppendEntries)
    leader_hint: Arc<RwLock<Option<u32>>>,
    registered_users: Arc<RwLock<HashMap<String, String>>>,
    // Idempotency for broadcasted client requests
    processed_ops: Arc<RwLock<std::collections::HashSet<String>>>,

}

impl NetNode {
    pub fn new(id: u32, peers: HashMap<u32, SocketAddr>) -> Self {
        let peers_arc = Arc::new(peers);
        let peer_ids: Vec<u32> = peers_arc.keys().cloned().collect();
        
        // Initialize next_index and match_index for all peers
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for peer_id in peer_ids {
            next_index.insert(peer_id, 1);  // Initialize to 1 (will be updated when becoming leader)
            match_index.insert(peer_id, 0);  // Initialize to 0 for new leaders
        }

        Self {
            id,
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
        }
    }

    #[inline]
    fn clamp1(n: u64) -> u64 {
        if n == 0 { 1 } else { n }
    }

    #[inline]
    fn prev_ptr(next_index: u64, log: &[LogEntry]) -> (u64, u64) {
        let prev_log_index = next_index.saturating_sub(1); // never underflow
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            log.get(prev_log_index as usize - 1).map(|e| e.term).unwrap_or(0)
        };
        (prev_log_index, prev_log_term)
    }

    fn client_addr_for(id: u32) -> String {
        format!("127.0.0.1:{}", 9000 + id)
    }

    async fn forward_to_leader(&self, cmd_line: &str) -> anyhow::Result<String> {
        let lid = (*self.leader_hint.read().await)
            .ok_or_else(|| anyhow::anyhow!("no leader hint"))?;
        let addr = NetNode::client_addr_for(lid);

        let stream = TcpStream::connect(addr).await?;
        let (r, mut w) = stream.into_split();
        let mut reader = BufReader::new(r);

        // Read the 2-line banner
        let mut tmp = String::new();
        reader.read_line(&mut tmp).await?; // "Welcome..."
        tmp.clear();
        reader.read_line(&mut tmp).await?; // "Commands: ..."

        // Send the same command to the leader
        w.write_all(cmd_line.as_bytes()).await?;
        w.write_all(b"\n").await?;

        // Read one response burst (OK / NOT_LEADER / REDIRECT / multi-line list)
        // For our writes we only need 'OK' or a line.
        let mut buf = String::new();
        // Try to read a few lines quickly; stop if no data comes for a moment
        // (simple approach: read up to 8KB or until reader would block)
        // Here: read until first newline
        reader.read_line(&mut buf).await?;
        Ok(buf)
    }

    async fn start(self: Arc<Self>, listen_addr: SocketAddr) -> anyhow::Result<()> {
        // Start listener with retry
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

        // ✅ Start stdin input loop (moved here so it actually runs)
        {
            let input_node = self.clone();
            tokio::spawn(async move {
                use tokio::io::{self, AsyncBufReadExt};
                let mut lines = io::BufReader::new(io::stdin()).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if matches!(*input_node.state.read().await, RaftState::Leader) {
                        // Build new log entry
                        let mut log = input_node.log.write().await;
                        let term = *input_node.current_term.read().await;
                        let index = (log.len() as u64) + 1;
                        log.push(LogEntry { term, index, command: line.clone() });
                        drop(log);

                        info!("Node {}: appended new entry {}", input_node.id, line);

                        // (Optional) Send immediately to peers instead of waiting for heartbeat
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

        // Spawn accept loop with error handling
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

        // Connect to peers (outbound) with exponential backoff
        for (&peer_id, &addr) in self.peers.iter() {
            let node = self.clone();
            tokio::spawn(async move {
                let mut backoff = Duration::from_secs(1);
                const MAX_BACKOFF: Duration = Duration::from_secs(30);

                loop {
                    info!(
                        "Node {} attempting to connect to peer {} at {}",
                        node.id, peer_id, addr
                    );
                    match TcpStream::connect(addr).await {
                        Ok(stream) => {
                            info!(
                                "Node {} connected to peer {} at {}",
                                node.id, peer_id, addr
                            );
                            backoff = Duration::from_secs(1);
                            if let Err(e) = node.handle_outbound(peer_id, stream).await {
                                error!(
                                    "Outbound connection {} -> {} failed: {}",
                                    node.id, peer_id, e
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                "Connect to peer {} at {} failed: {}. Retrying in {:?}...",
                                peer_id, addr, e, backoff
                            );
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                        }
                    }
                }
            });
        }

        // Periodically check for missing outbound connections
        let reconnect_node = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await; // every 5 seconds
                let current_outbound = reconnect_node.outbound.lock().await;
                let connected: Vec<u32> = current_outbound.keys().cloned().collect();
                drop(current_outbound);

                for (&peer_id, &addr) in reconnect_node.peers.iter() {
                    if connected.contains(&peer_id) {
                        continue;
                    }

                    info!("Node {} detected missing connection to {}, retrying...", reconnect_node.id, peer_id);
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


        // Start election timer and raft logic loops
        let tnode = self.clone();
        tokio::spawn(async move { tnode.election_loop().await });

        // Start heartbeat loop
        let hnode = self.clone();
        tokio::spawn(async move { hnode.heartbeat_loop().await });

        // Apply-committer: runs on ALL nodes (leaders & followers)
        let applier = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(100)).await;
                applier.apply_committed_entries().await;
            }
        });

        // Start client API listener on a port (9000 + node id)
        let client_node = self.clone();
        tokio::spawn(async move {
            let client_addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + client_node.id)
                .parse()
                .expect("parse client addr");
            if let Err(e) = client_node.run_client_api(client_addr).await {
                error!("client API failed: {:?}", e);
            }
        });

        Ok(())
    }

    async fn handle_inbound(&self, stream: TcpStream) -> anyhow::Result<()> {
        let mut framed = FramedRead::new(stream, LengthDelimitedCodec::new());
        while let Some(frame_res) = framed.next().await {
            let frame = frame_res?; // BytesMut
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
        // register sender
        {
            let mut out = self.outbound.lock().await;
            out.insert(peer_id, tx.clone());
        }

        let result = async {
            while let Some(msg) = rx.recv().await {
                let payload = serde_json::to_vec(&msg)?;
                if let Err(e) = writer.send(Bytes::from(payload)).await {
                    // propagate so we can clean up and reconnect
                    return Err::<(), anyhow::Error>(e.into());
                }
            }
            Ok(())
        }.await;

        // ensure we drop the sender for this peer on any exit path
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
            // no connection yet
            debug!("Node {}: no outbound for {}", self.id, target);
        }
    }

    async fn handle_message(&self, message: RaftMessage) {
        match message {
            RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                info!("Node {} received vote request from {} for term {}", self.id, candidate_id, term);
                
                let log = self.log.read().await;
                let our_last_index = log.len() as u64;
                let our_last_term = log.last().map_or(0, |entry| entry.term);
                
                // Check if candidate's log is at least as up-to-date as ours
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
                
                // Handle vote request
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
                
                // Reply false if term < currentTerm
                if term < current_term {
                    info!("Node {} rejecting AppendEntries (term {} < {})", self.id, term, current_term);
                } else {
                    // Update term if needed
                    if term > current_term {
                        *self.current_term.write().await = term;
                        *self.state.write().await = RaftState::Follower;
                        *self.voted_for.write().await = None;
                    }
                    
                    // Reset heartbeat timer
                    *self.last_heartbeat.write().await = Instant::now();
                    
                    let mut log = self.log.write().await;
                    
                    // Check log consistency
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
                        // Truncate conflicting entries and append new ones
                        if prev_log_index < log.len() as u64 {
                            log.truncate(prev_log_index as usize);
                        }
                        log.extend_from_slice(&entries);
                        
                        // Update commit index
                        let mut commit_index = self.commit_index.write().await;
                        if leader_commit > *commit_index {
                            *commit_index = leader_commit.min(log.len() as u64);
                            info!("Node {} updated commit_index to {}", self.id, *commit_index);
                        }
                                                // Apply any newly committed entries on this follower as well
                        drop(commit_index); // release lock before await
                        drop(log);          // release lock before await
                        self.apply_committed_entries().await;
                        success = true;
                    }
                }
                
                // Send reply
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
                            *next_idx = Self::clamp1(next_idx.saturating_sub(1)); // clamp to ≥1
                            info!("Node {} decreased next_index for {} to {}", self.id, sender_id, *next_idx);
                        }
                    }
                }
            }
        }
    }

    // async fn handle_vote_request(&self, term: u64, candidate_id: u32) {

    // }

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
            // count granted votes
            let granted_votes = entry.values().filter(|&&v| v).count();
            let total = self.peers.len() + 1;
            info!("Node {}: term {} has {} granted votes out of {}", self.id, term, granted_votes, total);
        if granted_votes > total / 2 && matches!(*self.state.read().await, RaftState::Candidate) {
            *self.state.write().await = RaftState::Leader;
            info!("Node {} became leader for term {}", self.id, current_term);

            // Properly initialize replication state (Raft §5.2)
            let last_index = self.log.read().await.len() as u64;
            {
                let mut next_indices = self.next_index.write().await;
                let mut match_indices = self.match_index.write().await;
                for (&peer_id, _) in self.peers.iter() {
                    next_indices.insert(peer_id, last_index + 1); // 1-based, points to "next to send"
                    match_indices.insert(peer_id, 0);
                }
            }

            // send initial heartbeat
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
        // 2–4s timeout
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

        // reset & self vote
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

    // Apply committed log entries to state machine
        // Apply committed log entries to state machine
    async fn apply_committed_entries(&self) {
        let commit_index = *self.commit_index.read().await;
        let mut last_applied = self.last_applied.write().await;

        if commit_index > *last_applied {
            let log = self.log.read().await;
            for i in (*last_applied + 1)..=commit_index {
                if let Some(entry) = log.get(i as usize - 1) {
                    // Parse and apply: commands are simple strings like:
                    // "REGISTER <user> <ip>", "UNREGISTER <user>", or "SEND_PHOTO <photo_id> <file_path>"
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
                        Some("SEND_PHOTO") => {
                            if let (Some(photo_id), Some(file_path)) = (parts.next(), parts.next()) {
                                let photo_path = format!("photos/{}.jpg", photo_id);
                                match tokio::fs::read(file_path).await {
                                    Ok(image_data) => {
                                        tokio::fs::write(&photo_path, image_data).await.ok();
                                        info!("Node {}: Applied SEND_PHOTO {} from {} to {}", self.id, photo_id, file_path, photo_path);
                                    }
                                    Err(e) => {
                                        info!("Node {}: Failed to read photo file {}: {}", self.id, file_path, e);
                                    }
                                }
                            } else {
                                info!("Node {}: Malformed SEND_PHOTO command '{}'", self.id, entry.command);
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

    async fn receive_photo_stream(
        &self,
        reader: &mut BufReader<tokio::net::tcp::ReadHalf<'_>>,
        photo_id: &str,
        mut remaining: usize,
    ) -> anyhow::Result<String> {
        // Ensure photos dir exists
        fs::create_dir_all("photos").await?;

        let tmp_path = format!("photos/{}.part", photo_id);
        let final_path = format!("photos/{}.bin", photo_id);

        // Open file for writing
        let mut file = tokio::fs::File::create(&tmp_path).await?;

        // SHA256 hasher
        let mut hasher = Sha256::new();

        // Buffer for streaming
        let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer

        while remaining > 0 {
            let to_read = std::cmp::min(buf.len(), remaining);
            // read up to to_read bytes into buf[0..to_read]
            let n = reader.read(&mut buf[..to_read]).await?;
            if n == 0 {
                anyhow::bail!("unexpected EOF while reading photo (remaining {} bytes)", remaining);
            }
            // write to file
            file.write_all(&buf[..n]).await?;
            // update hash
            hasher.update(&buf[..n]);
            remaining -= n;
        }

        // flush & sync (optional)
        file.flush().await?;
        // Optionally sync data to disk:
        // use tokio::fs::File::sync_all - note std::fs::File has sync_all, tokio provides it via File::sync_all
        file.sync_all().await.ok();

        // compute hex checksum
        let digest = hasher.finalize();
        let checksum_hex = hex::encode(digest);

        // atomically rename part -> final
        tokio::fs::rename(&tmp_path, &final_path).await?;

        Ok(checksum_hex)
    }

    async fn heartbeat_loop(&self) {
        loop {
            // Heartbeat interval should be significantly less than election timeout but not too aggressive
            sleep(Duration::from_millis(500)).await;
            
            // Only leaders send heartbeats
            if !matches!(*self.state.read().await, RaftState::Leader) {
                continue;
            }
            let term = *self.current_term.read().await;
            let commit_index = *self.commit_index.read().await;
            let log = self.log.read().await;
            
            // Apply any newly committed entries
            self.apply_committed_entries().await;
            
            for (&peer_id, _) in self.peers.iter() {
                let next_index = {
                    let next_indices = self.next_index.read().await;
                    next_indices.get(&peer_id).copied().unwrap_or(1)
                };
                
                // Prepare entries to send
                let (prev_log_index, prev_log_term) = Self::prev_ptr(next_index, &log);
                
                // Get entries starting from next_index (avoid underflow)
                let start_idx = next_index.saturating_sub(1);            // u64
                let start_usize = usize::try_from(start_idx).unwrap_or(0);
                let entries: Vec<LogEntry> = if next_index == 0 || start_usize > log.len() {
                    Vec::new()
                } else {
                    log[start_usize..].to_vec()
                };
                
                // Send AppendEntries RPC
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
        // 1️⃣ Update follower’s replication state
        {
            let mut next_indices = self.next_index.write().await;
            let mut match_indices = self.match_index.write().await;
            match_indices.insert(peer_id, acked_index);
            next_indices.insert(peer_id, acked_index + 1);
        }

        // 2️⃣ Compute possible majority commit index
        let mut all_matches: Vec<u64> = {
            let m = self.match_index.read().await;
            m.values().copied().collect()
        };
        let leader_last = self.log.read().await.len() as u64;
        all_matches.push(leader_last); // include leader
        all_matches.sort_unstable();

        let majority_idx = all_matches[all_matches.len() / 2];
        if majority_idx == 0 {
            return;
        }

        // 3️⃣ Commit only entries from current term (Raft §5.4.2)
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

    // Client API: a simple TCP line protocol for issuing commands
    // ================================================================
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
        w.write_all(b"Commands: LEADER | REGISTER <user> <ip> | UNREGISTER <user> | LIST | SHOW_USERS | SUBMIT <op_id> <command...>\n").await?;

        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            let cmd = line.trim();
            let is_leader = matches!(*self.state.read().await, RaftState::Leader);
            let mut parts = cmd.split_whitespace();

            match parts.next() {
                Some("LEADER") => {
                    if is_leader {
                        w.write_all(format!("LEADER {}\n", self.id).as_bytes()).await?;
                    } else if let Some(lid) = *self.leader_hint.read().await {
                        let addr = NetNode::client_addr_for(lid);
                        w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                    } else {
                        w.write_all(b"NOT_LEADER\n").await?;
                    }
                }

                Some("REGISTER") => {
                    if !is_leader {
                        match self.forward_to_leader(cmd).await {
                            Ok(reply) => {
                                w.write_all(reply.as_bytes()).await?;
                            }
                            Err(_) => {
                                // fallback to hint if we can
                                if let Some(lid) = *self.leader_hint.read().await {
                                    let addr = NetNode::client_addr_for(lid);
                                    w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                                } else {
                                    w.write_all(b"NOT_LEADER\n").await?;
                                }
                            }
                        }
                        continue;
                    }
                    let user = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: REGISTER <user> <ip>\n").await?;
                            continue;
                        }
                    };
                    let ip = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: REGISTER <user> <ip>\n").await?;
                            continue;
                        }
                    };

                    let command = format!("REGISTER {} {}", user, ip);
                    self.append_and_replicate(command).await;
                    w.write_all(b"OK\n").await?;
                }

                Some("UNREGISTER") => {
                    if !is_leader {
                        match self.forward_to_leader(cmd).await {
                            Ok(reply) => {
                                w.write_all(reply.as_bytes()).await?;
                            }
                            Err(_) => {
                                // fallback to hint if we can
                                if let Some(lid) = *self.leader_hint.read().await {
                                    let addr = NetNode::client_addr_for(lid);
                                    w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                                } else {
                                    w.write_all(b"NOT_LEADER\n").await?;
                                }
                            }
                        }
                        continue;
                    }
                    let user = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: UNREGISTER <user>\n").await?;
                            continue;
                        }
                    };
                    let command = format!("UNREGISTER {}", user);
                    self.append_and_replicate(command).await;
                    w.write_all(b"OK\n").await?;
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
                }
                
                Some("SHOW_USERS") => {
                    let users = self.registered_users.read().await;
                    if users.is_empty() {
                        w.write_all(b"(empty)\n").await?;
                    } else {
                        for (u, ip) in users.iter() {
                            w.write_all(format!("{} {}\n", u, ip).as_bytes()).await?;
                        }
                    }
                }

                                Some("SUBMIT") => {
                    // Usage: SUBMIT <op_id> <command...>
                    let op_id = match parts.next() {
                        Some(s) => s.to_string(),
                        None => {
                            w.write_all(b"Usage: SUBMIT <op_id> <command...>\n").await?;
                            continue;
                        }
                    };
                    let rest = parts.collect::<Vec<_>>().join(" ");
                    if rest.is_empty() {
                        w.write_all(b"Usage: SUBMIT <op_id> <command...>\n").await?;
                        continue;
                    }

                    // Idempotency: if we've seen this op_id, acknowledge
                    {
                        let seen = self.processed_ops.read().await;
                        if seen.contains(&op_id) {
                            w.write_all(b"OK\n").await?;
                            continue;
                        }
                    }

                    let is_leader = matches!(*self.state.read().await, RaftState::Leader);
                    if !is_leader {
                        if let Some(lid) = *self.leader_hint.read().await {
                            let addr = NetNode::client_addr_for(lid);
                            w.write_all(format!("REDIRECT {}\n", addr).as_bytes()).await?;
                        } else {
                            w.write_all(b"NOT_LEADER\n").await?;
                        }
                        continue;
                    }

                    // Leader path: mark op_id seen, then append+replicate the payload
                    {
                        let mut seen = self.processed_ops.write().await;
                        seen.insert(op_id);
                    }
                    self.append_and_replicate(rest).await;
                    w.write_all(b"OK\n").await?;
                }


                Some("SEND_PHOTO") => {
                    // Two supported variants:
                    // 1) SEND_PHOTO <photo_id> <file_path>   (existing behavior: server reads local file_path)
                    // 2) SEND_PHOTO <photo_id> <size> <carrier>  (new): client streams <size> bytes after the header
                    if let Some(photo_id) = parts.next() {
                        if let Some(second) = parts.next() {
                            // If second token parses as an integer -> treat as streamed upload size
                            if let Ok(size) = second.parse::<usize>() {
                                // optional carrier token (we ignore for now)
                                let _carrier = parts.next(); // may be Some("png") etc.
                                // Call helper to receive exactly `size` bytes from the TCP stream (reader)
                                match self.receive_photo_stream(&mut reader, photo_id, size).await {
                                    Ok(checksum) => {
                                        let resp = format!("OK {}\n", checksum);
                                        w.write_all(resp.as_bytes()).await?;
                                        info!("Node {}: received photo {} ({} bytes), checksum {}", self.id, photo_id, size, checksum);
                                    }
                                    Err(e) => {
                                        let err = format!("ERR upload failed: {}\n", e);
                                        w.write_all(err.as_bytes()).await?;
                                        error!("Node {}: receive_photo_stream failed: {}", self.id, e);
                                    }
                                }
                            } else {
                                // Not numeric → treat as file_path on server
                                let file_path = second;
                                let photo_path = format!("photos/{}.jpg", photo_id);
                                match tokio::fs::read(file_path).await {
                                    Ok(image_data) => {
                                        tokio::fs::write(&photo_path, image_data).await?;
                                        info!("Node {}: Saved photo {} from {} to {}", self.id, photo_id, file_path, photo_path);
                                        w.write_all(b"OK\n").await?;
                                    }
                                    Err(e) => {
                                        info!("Node {}: Failed to read photo file {}: {}", self.id, file_path, e);
                                        let err = format!("ERR could not read file: {}\n", e);
                                        w.write_all(err.as_bytes()).await?;
                                    }
                                }
                            }
                        } else {
                            info!("Node {}: Malformed SEND_PHOTO command (missing args)", self.id);
                            w.write_all(b"ERR malformed SEND_PHOTO\n").await?;
                        }
                    } else {
                        info!("Node {}: Malformed SEND_PHOTO command (missing photo_id)", self.id);
                        w.write_all(b"ERR malformed SEND_PHOTO\n").await?;
                    }
                }


                Some(unknown) => {
                    let s = format!("ERR unknown command: {}\n", unknown);
                    w.write_all(s.as_bytes()).await?;
                }

                None => {}
            }
        }
        Ok(())
    }

    // Append a command to this leader's log and replicate to peers
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
    let node = Arc::new(NetNode::new(args.id, peers_map));
    node.clone().start(listen).await?;
    // keep alive
    loop { sleep(Duration::from_secs(3600)).await; }
}
