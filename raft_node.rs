pub struct RaftNode {
    pub id: u32,
    pub state: Arc<RwLock<RaftState>>,
    pub current_term: Arc<RwLock<u64>>,
    pub voted_for: Arc<RwLock<Option<u32>>>,
    pub log: Arc<RwLock<Vec<LogEntry>>>,
    pub commit_index: Arc<RwLock<u64>>,
    pub last_applied: Arc<RwLock<u64>>,
    
    // Leader state
    pub next_index: Arc<RwLock<HashMap<u32, u64>>>,
    pub match_index: Arc<RwLock<HashMap<u32, u64>>>,
    
    // Peers and networking
    pub peers: Vec<u32>,
    pub last_heartbeat: Arc<RwLock<Instant>>,
    
    // Cloud P2P specific state
    pub discovery_service: Arc<RwLock<DiscoveryService>>,
    pub load_balancer: Arc<RwLock<LoadBalancer>>,
    
    // Communication channels
    pub vote_tx: mpsc::UnboundedSender<(RequestVoteRequest, mpsc::UnboundedSender<RequestVoteResponse>)>,
    pub append_tx: mpsc::UnboundedSender<(AppendEntriesRequest, mpsc::UnboundedSender<AppendEntriesResponse>)>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryService {
    pub registered_users: HashMap<String, UserInfo>,
}

#[derive(Debug, Clone)]
pub struct UserInfo {
    pub user_id: String,
    pub ip: String,
    pub last_seen: Instant,
}

#[derive(Debug, Clone)]
pub struct LoadBalancer {
    pub server_loads: HashMap<u32, f64>,
    pub current_leader: Option<u32>,
}
