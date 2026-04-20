//! Plugin configuration — parsed from `config.toml`.

use crate::logging::LoggingConfig;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub homecore: HomecoreSection,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default, rename = "thermostat")]
    pub thermostats: Vec<ThermostatEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HomecoreSection {
    pub plugin_id: String,
    pub broker_host: String,
    pub broker_port: u16,
    /// MQTT credential. Empty = anonymous (dev broker default).
    /// The broker uses `plugin_id` as the username; this is just the password.
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u64,
}

fn default_heartbeat_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ThermostatEntry {
    pub id: String,
    pub name: String,

    pub sensor_device_ids: Vec<String>,
    #[serde(default = "default_sensor_attribute")]
    pub sensor_attribute: String,
    #[serde(default = "default_aggregation")]
    pub aggregation: String,

    pub setpoint: f64,
    #[serde(default = "default_hysteresis")]
    pub hysteresis: f64,
    #[serde(default = "default_mode")]
    pub mode: String,

    #[serde(default)]
    pub actuator_device_id: String,
    #[serde(default)]
    pub actuator_on_cmd: Option<serde_json::Value>,
    #[serde(default)]
    pub actuator_off_cmd: Option<serde_json::Value>,

    #[serde(default)]
    pub min_on_secs: u64,
    #[serde(default)]
    pub min_off_secs: u64,
}

fn default_sensor_attribute() -> String {
    "temperature".into()
}
fn default_aggregation() -> String {
    "average".into()
}
fn default_hysteresis() -> f64 {
    1.0
}
fn default_mode() -> String {
    "off".into()
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow!("Cannot read config file {path}: {e}"))?;
        let cfg: Config =
            toml::from_str(&text).map_err(|e| anyhow!("Config parse error in {path}: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.homecore.plugin_id.is_empty() {
            return Err(anyhow!("homecore.plugin_id is required"));
        }
        for t in &self.thermostats {
            if t.id.is_empty() {
                return Err(anyhow!("thermostat id is required"));
            }
            if !matches!(t.mode.as_str(), "heat" | "cool" | "off") {
                return Err(anyhow!(
                    "thermostat {}: mode must be heat|cool|off (got {})",
                    t.id,
                    t.mode
                ));
            }
            if !matches!(t.aggregation.as_str(), "average" | "min" | "max") {
                return Err(anyhow!(
                    "thermostat {}: aggregation must be average|min|max (got {})",
                    t.id,
                    t.aggregation
                ));
            }
            if t.hysteresis < 0.0 {
                return Err(anyhow!(
                    "thermostat {}: hysteresis must be non-negative",
                    t.id
                ));
            }
        }
        Ok(())
    }
}
