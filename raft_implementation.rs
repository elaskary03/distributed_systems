impl RaftNode {
    pub fn new(id: u32, peers: Vec<u32>) -> Self {
        let (vote_tx, vote_rx) = mpsc::unbounded_channel();
        let (append_tx, append_rx) = mpsc::unbounded_channel();
        
        Self {
            id,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(vec![])),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            next_index: Arc::new(RwLock::new(HashMap::new())),
            match_index: Arc::new(RwLock::new(HashMap::new())),
            peers,
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            discovery_service: Arc::new(RwLock::new(DiscoveryService {
                registered_users: HashMap::new(),
            })),
            load_balancer: Arc::new(RwLock::new(LoadBalancer {
                server_loads: HashMap::new(),
                current_leader: None,
            })),
            vote_tx,
            append_tx,
        }
    }
    
    pub async fn start(&self) {
        // Start election timeout
        let election_timeout = self.clone();
        tokio::spawn(async move {
            election_timeout.election_timeout_loop().await;
        });
        
        // Start heartbeat (if leader)
        let heartbeat = self.clone();
        tokio::spawn(async move {
            heartbeat.heartbeat_loop().await;
        });
        
        // Start log application
        let log_applier = self.clone();
        tokio::spawn(async move {
            log_applier.apply_log_entries().await;
        });
    }

    pub async fn apply_log_entries(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let commit_index = *self.commit_index.read().await;
            let mut last_applied = self.last_applied.write().await;

            while *last_applied < commit_index {
                *last_applied += 1;
                let entry = {
                    let log = self.log.read().await;
                    log.get((*last_applied - 1) as usize).cloned()
                };

                if let Some(e) = entry {
                    self.apply_log_entry(e).await;
                }
            }
        }
    }

    pub async fn apply_log_entry(&self, entry: LogEntry) {
        match entry.command {
            Command::RegisterUser { user_id, ip } => {
                self.discovery_service.write().await.registered_users.insert(
                    user_id.clone(),
                    UserInfo {
                        user_id,
                        ip,
                        last_seen: Instant::now(),
                    },
                );
                println!("✅ Node {} applied RegisterUser", self.id);
            }
            Command::UnregisterUser { user_id } => {
                self.discovery_service
                    .write()
                    .await
                    .registered_users
                    .remove(&user_id);
                println!("❌ Node {} applied UnregisterUser", self.id);
            }
            Command::UpdateLoadBalancing { server_id, load } => {
                self.load_balancer
                    .write()
                    .await
                    .server_loads
                    .insert(server_id, load);
            }
            Command::EncryptImage { image_id, metadata } => {
                println!(
                    "🖼️ Node {} applied EncryptImage {} metadata={}",
                    self.id, image_id, metadata
                );
            }
        }
    }
    
        async fn election_timeout_loop(&self) {
            loop {
                // Randomize between 1500ms and 3000ms to reduce split votes
                let timeout_duration = Duration::from_millis(1500 + rand::random::<u64>() % 1500);
                tokio::time::sleep(timeout_duration).await;
            
            let last_heartbeat = *self.last_heartbeat.read().await;
            let state = self.state.read().await.clone();
            
            if matches!(state, RaftState::Follower | RaftState::Candidate) {
                if last_heartbeat.elapsed() > timeout_duration {
                    self.start_election().await;
                }
            }
        }
    }
    
    async fn start_election(&self) {
        println!("Node {} starting election", self.id);
        
        // Become candidate
        *self.state.write().await = RaftState::Candidate;
        *self.current_term.write().await += 1;
        *self.voted_for.write().await = Some(self.id);
        *self.last_heartbeat.write().await = Instant::now();
        
        let current_term = *self.current_term.read().await;
        let log = self.log.read().await;
        let last_log_index = log.len() as u64;
        let last_log_term = log.last().map(|entry| entry.term).unwrap_or(0);
        
        let vote_request = RequestVoteRequest {
            term: current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };
        
        let mut votes = 1; // Vote for self
        let majority = (self.peers.len() + 1) / 2 + 1;
        
        // Send vote requests to all peers
        for peer_id in &self.peers {
            if let Ok(response) = self.send_vote_request(*peer_id, vote_request.clone()).await {
                if response.vote_granted {
                    votes += 1;
                }
                
                // Check if we got a higher term
                if response.term > current_term {
                    *self.current_term.write().await = response.term;
                    *self.voted_for.write().await = None;
                    *self.state.write().await = RaftState::Follower;
                    return;
                }
            }
        }
        
        // Check if we won the election
        if votes >= majority {
            self.become_leader().await;
        }
    }
    
    async fn become_leader(&self) {
        println!("Node {} became leader for term {}", self.id, *self.current_term.read().await);
        
        *self.state.write().await = RaftState::Leader;
        
        // Initialize leader state
        let log_len = self.log.read().await.len() as u64 + 1;
        let mut next_index = self.next_index.write().await;
        let mut match_index = self.match_index.write().await;
        
        for peer_id in &self.peers {
            next_index.insert(*peer_id, log_len);
            match_index.insert(*peer_id, 0);
        }
        
        // Update load balancer
        self.load_balancer.write().await.current_leader = Some(self.id);
        
        // Send initial heartbeat
        self.send_heartbeat().await;
    }
    
    async fn heartbeat_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            
            let state = self.state.read().await.clone();
            if matches!(state, RaftState::Leader) {
                self.send_heartbeat().await;
            }
        }
    }
    
    async fn send_heartbeat(&self) {
        let current_term = *self.current_term.read().await;
        let commit_index = *self.commit_index.read().await;
        
        for peer_id in &self.peers {
            let next_idx = self.next_index.read().await.get(peer_id).copied().unwrap_or(1);
            let log = self.log.read().await;
            
            let prev_log_index = if next_idx > 1 { next_idx - 1 } else { 0 };
            let prev_log_term = if prev_log_index > 0 {
                log.get((prev_log_index - 1) as usize).map(|e| e.term).unwrap_or(0)
            } else {
                0
            };
            
            let entries = if next_idx <= log.len() as u64 {
                log[next_idx as usize - 1..].to_vec()
            } else {
                vec![]
            };
            
            let append_request = AppendEntriesRequest {
                term: current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: commit_index,
            };
            
            // Send append entries (heartbeat)
            if let Ok(response) = self.send_append_entries(*peer_id, append_request).await {
                if response.term > current_term {
                    *self.current_term.write().await = response.term;
                    *self.voted_for.write().await = None;
                    *self.state.write().await = RaftState::Follower;
                    return;
                }
                
                if response.success {
                    let new_match_index = prev_log_index + entries.len() as u64;
                    self.next_index.write().await.insert(*peer_id, new_match_index + 1);
                    self.match_index.write().await.insert(*peer_id, new_match_index);

                    // 🔥 Try to advance commit_index if majority replicated
                    let mut match_indexes: Vec<u64> = self.match_index.read().await.values().cloned().collect();
                    match_indexes.push(self.log.read().await.len() as u64);
                    match_indexes.sort_unstable();
                    let majority_pos = match_indexes.len() / 2;
                    let new_commit = match_indexes[majority_pos];

                    let current_commit = *self.commit_index.read().await;
                    if new_commit > current_commit {
                        *self.commit_index.write().await = new_commit;
                        println!("🔥 Node {} committed up to index {}", self.id, new_commit);
                    }
                } else {
                    // Decrement next_index and retry
                    let next_index = self.next_index.read().await.get(peer_id).copied().unwrap_or(1);
                    if next_index > 1 {
                        self.next_index.write().await.insert(*peer_id, next_index - 1);
                    }
                }
            }
        }
    }
    
    // Cloud P2P specific methods
    pub async fn register_user(&self, user_id: String, ip: String) -> Result<(), String> {
        let command = Command::RegisterUser { user_id, ip };
        self.append_command(command).await
    }
    
    pub async fn update_server_load(&self, server_id: u32, load: f64) -> Result<(), String> {
        let command = Command::UpdateLoadBalancing { server_id, load };
        self.append_command(command).await
    }
    
    async fn append_command(&self, command: Command) -> Result<(), String> {
        let state = self.state.read().await.clone();
        if !matches!(state, RaftState::Leader) {
            return Err("Not the leader".to_string());
        }
        
        let current_term = *self.current_term.read().await;
        let mut log = self.log.write().await;
        let index = log.len() as u64 + 1;
        
        let entry = LogEntry {
            term: current_term,
            index,
            command,
        };
        
        log.push(entry);
        Ok(())
    }
}
