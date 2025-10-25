use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock, Mutex};
use std::sync::Arc;
use tokio::time::{Duration, Instant, sleep};
use tracing::{info, debug};
use once_cell::sync::Lazy;

static NODE_REGISTRY: Lazy<Mutex<Vec<Arc<RaftNode>>>> = Lazy::new(|| Mutex::new(Vec::new()));

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
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    RegisterUser { user_id: String, ip: String },
    UnregisterUser { user_id: String },
    NoOp, // For heartbeats
}

#[derive(Debug, Clone)]
pub struct RaftNode {
    pub id: u32,
    pub state: Arc<RwLock<RaftState>>,
    pub current_term: Arc<RwLock<u64>>,
    pub voted_for: Arc<RwLock<Option<u32>>>,
    pub log: Arc<RwLock<Vec<LogEntry>>>,
    pub commit_index: Arc<RwLock<u64>>,
    pub last_applied: Arc<RwLock<u64>>,
    pub peers: Vec<u32>,
    pub last_heartbeat: Arc<RwLock<Instant>>,
    
    // Discovery service state
    pub registered_users: Arc<RwLock<HashMap<String, String>>>, // user_id -> ip
    
    // Inter-node communication
    pub message_bus: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<RaftMessage>>>>,

    pub failure_simulator: FailureSimulator,

    pub votes_received: Arc<RwLock<HashMap<u64, HashMap<u32, bool>>>>, // term -> voter_id -> granted
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
        success: bool,
    },
}

#[derive(Debug, Clone)]
pub struct FailureSimulator {
    pub is_failed: Arc<RwLock<bool>>,
    pub failure_start: Arc<RwLock<Option<Instant>>>,
    pub failure_duration: Duration,
}

impl FailureSimulator {
    pub fn new() -> Self {
        Self {
            is_failed: Arc::new(RwLock::new(false)),
            failure_start: Arc::new(RwLock::new(None)),
            failure_duration: Duration::from_secs(20),
        }
    }
    
    pub async fn simulate_failure(&self) {
        *self.is_failed.write().await = true;
        *self.failure_start.write().await = Some(Instant::now());
        info!("🔥 Node entering failure state for {} seconds", self.failure_duration.as_secs());
    }
    
    pub async fn check_recovery(&self) -> bool {
        if let Some(start_time) = *self.failure_start.read().await {
            if start_time.elapsed() >= self.failure_duration {
                *self.is_failed.write().await = false;
                *self.failure_start.write().await = None;
                info!("✅ Node recovered from failure");
                return true;
            }
        }
        false
    }

    
    pub async fn is_failed(&self) -> bool {
        *self.is_failed.read().await
    }
}

impl RaftNode {
    pub fn new(id: u32, peers: Vec<u32>) -> Self {
        Self {
            id,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(vec![])),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            peers,
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            registered_users: Arc::new(RwLock::new(HashMap::new())),
            message_bus: Arc::new(Mutex::new(HashMap::new())),
            failure_simulator: FailureSimulator::new(),
            votes_received: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn start(&self, rx: mpsc::UnboundedReceiver<RaftMessage>) {
        info!("Node {} starting", self.id);
        
        // Start message handler
        let msg_handler = self.clone();
        tokio::spawn(async move {
            msg_handler.handle_messages(rx).await;
        });
        
        // Start election timer
        let election_timer = self.clone();
        tokio::spawn(async move {
            election_timer.election_timeout_loop().await;
        });
        
        // Start heartbeat (for leaders)
        let heartbeat = self.clone();
        tokio::spawn(async move {
            heartbeat.heartbeat_loop().await;
        });

        // Start failure simulation
        let failure_sim = self.clone();
        tokio::spawn(async move {
            failure_sim.failure_simulation_loop().await;
        });
    }
    
    async fn handle_messages(&self, mut rx: mpsc::UnboundedReceiver<RaftMessage>) {
        while let Some(message) = rx.recv().await {

            // Check if this node is in failure state
            if self.failure_simulator.is_failed().await {
                // Ignore all incoming messages during failure
                continue;
            }
            
            // Check for recovery
            if self.failure_simulator.check_recovery().await {
                info!("Node {} recovered and rejoining cluster", self.id);
    
                // Perform proper state synchronization
                self.sync_state_from_peers().await;
                
                // Reset heartbeat timer
                *self.last_heartbeat.write().await = Instant::now();
            }
        
            match message {
                RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                    let reply = self.handle_vote_request(term, candidate_id, last_log_index, last_log_term).await;
                    self.send_message(candidate_id, reply).await;
                }
                RaftMessage::VoteReply { term, vote_granted, sender_id } => {
                    self.handle_vote_reply(term, vote_granted, sender_id).await;
                }
                RaftMessage::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                    let reply = self.handle_append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit).await;
                    self.send_message(leader_id, reply).await;
                }
                RaftMessage::AppendReply { term, success } => {
                    self.handle_append_reply(term, success).await;
                }
            }
        }
    }
    
    async fn election_timeout_loop(&self) {
        loop {
            let timeout_duration = Duration::from_millis(300 + rand::random::<u64>() % 200);
            let tick = Duration::from_millis(50);
            let mut elapsed = Duration::ZERO;
            while elapsed < timeout_duration {
                sleep(tick).await;
                elapsed += tick;
                // Check if heartbeat received or node failed
                if self.failure_simulator.is_failed().await {
                    break;
                }
                let last_heartbeat = *self.last_heartbeat.read().await;
                if last_heartbeat.elapsed() < timeout_duration {
                    elapsed = Duration::ZERO;
                }
            }
            // Check if the node recovered
            if self.failure_simulator.check_recovery().await {
                info!("🚀 Node {} rejoining cluster after recovery", self.id);
                self.sync_state_from_peers().await;
                *self.last_heartbeat.write().await = Instant::now();
                info!("✅ Node {} synchronized and READY", self.id);
            }
            // Skip election actions while failed
            if self.failure_simulator.is_failed().await {
                continue;
            }
            // Election timeout logic
            let last_heartbeat = *self.last_heartbeat.read().await;
            let state = self.state.read().await.clone();
            if matches!(state, RaftState::Follower) {
                if last_heartbeat.elapsed() > timeout_duration {
                    info!("Node {} election timeout, starting election", self.id);
                    self.start_election().await;
                }
            } else if matches!(state, RaftState::Candidate) {
                // If candidate did not win, step down after timeout
                info!("Node {}: Candidate timed out, stepping down to follower", self.id);
                *self.state.write().await = RaftState::Follower;
                *self.voted_for.write().await = None;
                self.votes_received.write().await.clear();
            }
        }
    }

    async fn start_election(&self) {
        info!("Node {} starting election", self.id);
        *self.state.write().await = RaftState::Candidate;
        *self.current_term.write().await += 1;
        *self.voted_for.write().await = Some(self.id);
        *self.last_heartbeat.write().await = Instant::now();
        let current_term = *self.current_term.read().await;
        let log = self.log.read().await;
        let last_log_index = log.len() as u64;
        let last_log_term = log.last().map(|entry| entry.term).unwrap_or(0);
        let vote_request = RaftMessage::RequestVote {
            term: current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };
        // Reset votes for this term
        let mut votes = self.votes_received.write().await;
        votes.insert(current_term, HashMap::new());
        votes.get_mut(&current_term).unwrap().insert(self.id, true); // vote for self
        drop(votes);
        // Send vote requests to all peers
        for peer_id in &self.peers {
            self.send_message(*peer_id, vote_request.clone()).await;
        }
    }
    
    async fn handle_vote_request(&self, term: u64, candidate_id: u32, last_log_index: u64, last_log_term: u64) -> RaftMessage {
        let mut current_term = self.current_term.write().await;
        let mut voted_for = self.voted_for.write().await;
        let log = self.log.read().await;
        if term > *current_term {
            *current_term = term;
            *voted_for = None;
            *self.state.write().await = RaftState::Follower;
        }
        let up_to_date = {
            let last_index = log.len() as u64;
            let last_term = log.last().map(|e| e.term).unwrap_or(0);
            last_log_term > last_term || (last_log_term == last_term && last_log_index >= last_index)
        };
        let vote_granted = if term == *current_term && up_to_date {
            match *voted_for {
                None => {
                    *voted_for = Some(candidate_id);
                    *self.last_heartbeat.write().await = Instant::now();
                    true
                }
                Some(id) => id == candidate_id,
            }
        } else {
            false
        };
        if vote_granted {
            info!("Node {} voted for candidate {}", self.id, candidate_id);
        }
        RaftMessage::VoteReply {
            term: *current_term,
            vote_granted,
            sender_id: self.id,
        }
    }
    
    async fn handle_vote_reply(&self, term: u64, vote_granted: bool, sender_id: u32) {
        let current_term = *self.current_term.read().await;
        let state = self.state.read().await.clone();
        if term > current_term {
            *self.current_term.write().await = term;
            *self.voted_for.write().await = None;
            *self.state.write().await = RaftState::Follower;
            return;
        }
        if matches!(state, RaftState::Candidate) && term == current_term {
            let mut votes = self.votes_received.write().await;
            let entry = votes.entry(term).or_insert_with(HashMap::new);
            // Track sender's vote
            entry.insert(sender_id, vote_granted);
            // Always count self
            entry.insert(self.id, true);
            let granted_votes = entry.values().filter(|&&v| v).count();
            let total = self.peers.len() + 1;
            info!("Node {}: term {} has {} granted votes out of {}", self.id, term, granted_votes, total);
            if granted_votes > total / 2 && matches!(*self.state.read().await, RaftState::Candidate) {
                self.become_leader().await;
            }
        }
    }

    async fn become_leader(&self) {
        let current_term = *self.current_term.read().await;
        // Only become leader if not already leader and term is highest
        if !matches!(*self.state.read().await, RaftState::Leader) {
            info!("Node {} became leader for term {}", self.id, current_term);
            *self.state.write().await = RaftState::Leader;
            // Send initial heartbeat
            self.send_heartbeat().await;
        }
    }
    
    async fn heartbeat_loop(&self) {
        loop {
            sleep(Duration::from_millis(100)).await;
            
            let state = self.state.read().await.clone();
            if matches!(state, RaftState::Leader) {
                self.send_heartbeat().await;
            }
        }
    }
    
    async fn send_heartbeat(&self) {
        let current_term = *self.current_term.read().await;
        let commit_index = *self.commit_index.read().await;
        
        let heartbeat = RaftMessage::AppendEntries {
            term: current_term,
            leader_id: self.id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: commit_index,
        };
        
        for peer_id in &self.peers {
            self.send_message(*peer_id, heartbeat.clone()).await;
        }
    }
    
    async fn handle_append_entries(&self, term: u64, leader_id: u32, prev_log_index: u64, prev_log_term: u64, entries: Vec<LogEntry>, leader_commit: u64) -> RaftMessage {
        let mut current_term = self.current_term.write().await;
        let mut log = self.log.write().await;
        let mut commit_index = self.commit_index.write().await;
        let mut last_applied = self.last_applied.write().await;
        let mut success = false;
        if term > *current_term {
            *current_term = term;
            *self.voted_for.write().await = None;
            *self.state.write().await = RaftState::Follower;
        }
        if term >= *current_term {
            *current_term = term;
            *self.voted_for.write().await = None;
            *self.state.write().await = RaftState::Follower;
            *self.last_heartbeat.write().await = Instant::now();
            // Log consistency check
            if prev_log_index == 0 || (log.len() as u64 >= prev_log_index && (prev_log_index == 0 || log.get((prev_log_index-1) as usize).map(|e| e.term) == Some(prev_log_term))) {
                // Append new entries
                for entry in &entries {
                    if (entry.index as usize) < log.len() {
                        log[entry.index as usize] = entry.clone();
                    } else {
                        log.push(entry.clone());
                    }
                }
                // Update commit index
                if leader_commit > *commit_index {
                    *commit_index = std::cmp::min(leader_commit, log.len() as u64);
                }
                // Apply committed entries
                while *last_applied < *commit_index {
                    *last_applied += 1;
                    // Apply log[*last_applied as usize - 1] if needed
                }
                success = true;
                if !entries.is_empty() {
                    info!("Node {} received {} log entries from leader {}", self.id, entries.len(), leader_id);
                }
            }
        }
        RaftMessage::AppendReply {
            term: *current_term,
            success,
        }
    }
    
    async fn handle_append_reply(&self, term: u64, success: bool) {
        if term > *self.current_term.read().await {
            *self.current_term.write().await = term;
            *self.voted_for.write().await = None;
            *self.state.write().await = RaftState::Follower;
        }
        
        if success {
            debug!("Append entries successful");
        }
    }
    
    async fn send_message(&self, target_id: u32, message: RaftMessage) {

        if self.failure_simulator.is_failed().await {
        // Ignore all outgoing messages during failure
        return;
        }
        let bus = self.message_bus.lock().await;
        if let Some(sender) = bus.get(&target_id) {
            let _ = sender.send(message);
        }
    }
    
    async fn failure_simulation_loop(&self) {
    loop {
        // Wait 10-30 seconds between potential failures
        let wait_time = Duration::from_secs(10 + rand::random::<u64>() % 20);
        sleep(wait_time).await;
        
        // 30% chance of triggering failure (adjust as needed)
        if rand::random::<f64>() < 0.3 && !self.failure_simulator.is_failed().await {
            self.failure_simulator.simulate_failure().await;
            }
        }
    }

    // Cloud P2P specific methods
    pub async fn register_user(&self, user_id: String, ip: String) -> Result<(), String> {
        let state = self.state.read().await.clone();
        if !matches!(state, RaftState::Leader) {
            return Err("Not the leader".to_string());
        }
        
        self.registered_users.write().await.insert(user_id.clone(), ip.clone());
        info!("Leader {} registered user {} at {}", self.id, user_id, ip);
        Ok(())
    }
    
    pub async fn get_users(&self) -> HashMap<String, String> {
        self.registered_users.read().await.clone()
    }
    
    pub async fn get_state_info(&self) -> (RaftState, u64, Option<u32>) {
        let state = self.state.read().await.clone();
        let term = *self.current_term.read().await;
        let voted_for = *self.voted_for.read().await;
        (state, term, voted_for)
    }

    async fn sync_state_from_peers(&self) {
        info!("Node {} syncing state from peers after recovery", self.id);
        // Always step down to follower and clear leadership/candidate claim
        *self.state.write().await = RaftState::Follower;
        *self.voted_for.write().await = None;
        self.votes_received.write().await.clear();
        // Query peers for their current term and set ours to the highest
        let mut highest_term = 0;
        for peer_id in &self.peers {
            if let Some(peer_term) = self.query_peer_term(*peer_id).await {
                if peer_term > highest_term {
                    highest_term = peer_term;
                }
            }
        }
        // Always set state to follower, even if term is not updated
        let mut current_term = self.current_term.write().await;
        if *current_term < highest_term {
            *current_term = highest_term;
        }
        *self.state.write().await = RaftState::Follower;
        *self.voted_for.write().await = None;
        self.votes_received.write().await.clear();
        info!("Node {} forcibly stepped down to follower for term {} after recovery", self.id, *current_term);
        info!("Node {} synchronized term to {} after recovery", self.id, *current_term);
    }

    // Simulate querying a peer for its current term
    async fn query_peer_term(&self, peer_id: u32) -> Option<u64> {
        let registry = NODE_REGISTRY.lock().await;
        for node in registry.iter() {
            if node.id == peer_id {
                return Some(*node.current_term.read().await);
            }
        }
        None
    }

    pub async fn sync_discovery_service(&self, peer_users: HashMap<String, String>) {
        let mut users = self.registered_users.write().await;
        
        // Merge peer state with our state
        for (user_id, ip) in peer_users {
            users.insert(user_id.clone(), ip.clone());
            info!("Synced user {} at {} from peers", user_id, ip);
        }
        
        info!("Discovery service state synchronized");
    }


}

// Test function
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();    
    
    info!("Starting Raft cluster simulation");
    
    // Create 3 nodes
    let node1 = Arc::new(RaftNode::new(1, vec![2, 3]));
    let node2 = Arc::new(RaftNode::new(2, vec![1, 3]));
    let node3 = Arc::new(RaftNode::new(3, vec![1, 2]));
    
    // Register nodes
    {
        let mut registry = NODE_REGISTRY.lock().await;
        registry.push(node1.clone());
        registry.push(node2.clone());
        registry.push(node3.clone());
    }

    // Create communication channels
    let (tx1, rx1) = mpsc::unbounded_channel();
    let (tx2, rx2) = mpsc::unbounded_channel();
    let (tx3, rx3) = mpsc::unbounded_channel();
    
    // Setup message bus
    {
        let mut bus1 = node1.message_bus.lock().await;
        bus1.insert(2, tx2.clone());
        bus1.insert(3, tx3.clone());
        
        let mut bus2 = node2.message_bus.lock().await;
        bus2.insert(1, tx1.clone());
        bus2.insert(3, tx3.clone());
        
        let mut bus3 = node3.message_bus.lock().await;
        bus3.insert(1, tx1.clone());
        bus3.insert(2, tx2.clone());
    }
    
    // Start all nodes
    node1.start(rx1).await;
    node2.start(rx2).await;
    node3.start(rx3).await;
    
    // Let the election happen
    sleep(Duration::from_secs(2)).await;
    
    // Test leader election
    for node in [&node1, &node2, &node3] {
        let (state, term, voted_for) = node.get_state_info().await;
        info!("Node {}: State={:?}, Term={}, VotedFor={:?}", node.id, state, term, voted_for);
    }
    
    // Find and test the leader
    for node in [&node1, &node2, &node3] {
        let (state, _, _) = node.get_state_info().await;
        if matches!(state, RaftState::Leader) {
            info!("Testing user registration with leader {}", node.id);
            let _ = node.register_user("alice".to_string(), "192.168.1.100".to_string()).await;
            let _ = node.register_user("bob".to_string(), "192.168.1.101".to_string()).await;
            
            let users = node.get_users().await;
            info!("Registered users: {:?}", users);
            break;
        }
    }
    
    // Let it run for a bit
    sleep(Duration::from_secs(5)).await;
    info!("Test completed");

    ////////////////////// NODE FAILURE SIMULATION //////////////////////
    // Let the initial election happen
    sleep(Duration::from_secs(2)).await;

    // Run for 60 seconds to see failures and recoveries
    for i in 0..12 {
        sleep(Duration::from_secs(5)).await;
        
        info!("=== Status Check {} ===", i + 1);
        for node in [&node1, &node2, &node3] {
            let (state, term, _) = node.get_state_info().await;
            let is_failed = node.failure_simulator.is_failed().await;
            let status = if is_failed { "FAILED" } else { "ACTIVE" };
            info!("Node {}: State={:?}, Term={}, Status={}", node.id, state, term, status);
        }
    }

    ////////////////////// NODE RECOVERY SIMULATION //////////////////////
    // Wait for nodes to recover
    sleep(Duration::from_secs(25)).await; // Wait past the 20-second failure window

    info!("=== After Recovery Check ===");
    for node in [&node1, &node2, &node3] {
        let (state, term, _) = node.get_state_info().await;
        let is_failed = node.failure_simulator.is_failed().await;
        let users = node.get_users().await;
        let status = if is_failed { "FAILED" } else { "RECOVERED" };
        info!("Node {}: State={:?}, Term={}, Status={}, Users={:?}", 
            node.id, state, term, status, users);
    }


}
