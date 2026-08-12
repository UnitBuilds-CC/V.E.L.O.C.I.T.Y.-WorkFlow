//! Dynamic configuration — runtime config changes without restart.

use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub enum ConfigValue { Bool(bool), Int(i64), Float(f64), String(String) }

pub struct DynamicConfig {
    values: RwLock<HashMap<String, ConfigValue>>,
    defaults: RwLock<HashMap<String, ConfigValue>>,
}

impl DynamicConfig {
    pub fn new() -> Self {
        let mut defaults = HashMap::new();
        defaults.insert("workflow.maxConcurrent".into(), ConfigValue::Int(1000));
        defaults.insert("workflow.executionTimeoutMs".into(), ConfigValue::Int(60000));
        defaults.insert("activity.maxRetries".into(), ConfigValue::Int(3));
        defaults.insert("activity.heartbeatTimeoutMs".into(), ConfigValue::Int(30000));
        defaults.insert("matching.forwardRate".into(), ConfigValue::Float(0.8));
        defaults.insert("namespace.maxWorkflows".into(), ConfigValue::Int(10000));
        defaults.insert("rateLimit.globalRps".into(), ConfigValue::Int(10000));
        Self { values: RwLock::new(HashMap::new()), defaults: RwLock::new(defaults) }
    }
    pub fn set(&self, key: &str, value: ConfigValue) { self.values.write().unwrap().insert(key.to_string(), value); }
    pub fn get(&self, key: &str) -> Option<ConfigValue> {
        if let Some(v) = self.values.read().unwrap().get(key) { return Some(v.clone()); }
        self.defaults.read().unwrap().get(key).cloned()
    }
    pub fn get_int(&self, key: &str) -> i64 { match self.get(key) { Some(ConfigValue::Int(v)) => v, _ => 0 } }
    pub fn get_bool(&self, key: &str) -> bool { matches!(self.get(key), Some(ConfigValue::Bool(true))) }
    pub fn get_float(&self, key: &str) -> f64 { match self.get(key) { Some(ConfigValue::Float(v)) => v, _ => 0.0 } }
    pub fn get_string(&self, key: &str) -> Option<String> { match self.get(key) { Some(ConfigValue::String(v)) => Some(v), _ => None } }
    pub fn key_count(&self) -> usize { self.values.read().unwrap().len() + self.defaults.read().unwrap().len() }

    /// List all config keys (overrides + defaults), sorted.
    pub fn list_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.values.read().unwrap().keys()
            .chain(self.defaults.read().unwrap().keys())
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
}

impl Default for DynamicConfig { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_defaults() {
        let config = DynamicConfig::new();
        assert_eq!(config.get_int("workflow.maxConcurrent"), 1000);
        assert_eq!(config.get_int("activity.maxRetries"), 3);
    }
    #[test]
    fn test_override() {
        let config = DynamicConfig::new();
        config.set("workflow.maxConcurrent", ConfigValue::Int(500));
        assert_eq!(config.get_int("workflow.maxConcurrent"), 500);
    }
    #[test]
    fn test_bool_config() {
        let config = DynamicConfig::new();
        config.set("feature.newEngine", ConfigValue::Bool(true));
        assert!(config.get_bool("feature.newEngine"));
        assert!(!config.get_bool("feature.nonexistent"));
    }
}
