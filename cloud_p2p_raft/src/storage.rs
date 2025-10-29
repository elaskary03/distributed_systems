use serde::{Serialize, Deserialize};
use std::{fs, io::Write, path::PathBuf};

#[derive(Serialize, Deserialize, Default)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<u32>,
    pub log: Vec<crate::LogEntry>,
    pub registered_users: std::collections::HashMap<String, String>,
}

impl PersistentState {
    pub fn load(path: &str) -> Self {
        let file = PathBuf::from(path);
        if file.exists() {
            match fs::read_to_string(&file) {
                Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
                Err(_) => Default::default(),
            }
        } else {
            Default::default()
        }
    }

    pub fn save(&self, path: &str) {
        if let Ok(json) = serde_json::to_string_pretty(self) {
            if let Ok(mut f) = fs::File::create(path) {
                let _ = f.write_all(json.as_bytes());
            }
        }
    }
}
