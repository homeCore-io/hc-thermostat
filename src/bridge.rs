//! Thermostat bridge — subscribes to sensors, runs the control loop, publishes
//! state and actuator commands.

use crate::config::{Config, ThermostatEntry};
use crate::control::{aggregate, compute_call_for, lockout_remaining};
use anyhow::Result;
use chrono::{DateTime, Utc};
use hc_types::device::{with_command_change_metadata, DeviceChange};
use plugin_sdk_rs::DevicePublisher;
use rumqttc::{AsyncClient, QoS};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

/// Per-thermostat runtime state — what we've last published.
#[derive(Debug, Clone)]
struct Runtime {
    cfg: ThermostatEntry,
    current_temperature: Option<f64>,
    call_for: String,
    actuator_state: bool,
    actuator_last_change: Option<DateTime<Utc>>,
    pending_call: Option<String>,
    lockout_until: Option<DateTime<Utc>>,
    /// Last actuator publish error — cleared on next successful publish.
    actuator_last_error: Option<ActuatorError>,
}

#[derive(Debug, Clone)]
struct ActuatorError {
    timestamp: DateTime<Utc>,
    message: String,
}

impl Runtime {
    fn new(cfg: ThermostatEntry) -> Self {
        Self {
            cfg,
            current_temperature: None,
            call_for: "idle".into(),
            actuator_state: false,
            actuator_last_change: None,
            pending_call: None,
            lockout_until: None,
            actuator_last_error: None,
        }
    }

    fn device_id(&self) -> String {
        format!("thermostat_{}", self.cfg.id)
    }

    /// Build the full state payload published to homecore/devices/{id}/state.
    fn state_payload(&self) -> Value {
        let ids: Vec<&str> = self.cfg.sensor_device_ids.iter().map(|s| s.as_str()).collect();
        json!({
            "sensor_ids": ids,
            "sensor_attribute": self.cfg.sensor_attribute,
            "aggregation": self.cfg.aggregation,
            "setpoint": self.cfg.setpoint,
            "hysteresis": self.cfg.hysteresis,
            "mode": self.cfg.mode,
            "actuator_device_id": self.cfg.actuator_device_id,
            "min_on_secs": self.cfg.min_on_secs,
            "min_off_secs": self.cfg.min_off_secs,

            "current_temperature": self.current_temperature,
            "call_for": self.call_for,
            "actuator_state": self.actuator_state,
            "actuator_last_change": self.actuator_last_change.map(|t| t.to_rfc3339()),
            "pending_call": self.pending_call,
            "lockout_until": self.lockout_until.map(|t| t.to_rfc3339()),
            "actuator_last_error": self.actuator_last_error.as_ref().map(|e| json!({
                "timestamp": e.timestamp.to_rfc3339(),
                "message": e.message,
            })),
            "last_update": Utc::now().to_rfc3339(),
        })
    }
}

/// Shared plugin state. Wrapped in Arc<Mutex<...>> because commands and state
/// messages arrive via separate callback threads.
pub struct Bridge {
    /// Thermostat_id → Runtime
    thermostats: HashMap<String, Runtime>,
    /// Sensor device_id → last numeric reading (keyed by the configured
    /// `sensor_attribute`).
    sensor_cache: HashMap<String, f64>,
    config_path: String,
}

pub struct BridgeHandle {
    inner: Arc<Mutex<Bridge>>,
    publisher: DevicePublisher,
    mqtt: AsyncClient,
    plugin_id: String,
    /// Sync-readable snapshot of thermostat configs — updated whenever the
    /// bridge mutates thermostats. Used by management custom handlers
    /// (`get_thermostats`) which must answer synchronously without blocking
    /// the MQTT event loop.
    snapshot: Arc<std::sync::Mutex<Vec<Value>>>,
}

impl BridgeHandle {
    pub async fn new(
        cfg: &Config,
        publisher: DevicePublisher,
        mqtt: AsyncClient,
        config_path: &str,
        snapshot: Arc<std::sync::Mutex<Vec<Value>>>,
    ) -> Result<Self> {
        let thermostats = cfg
            .thermostats
            .iter()
            .cloned()
            .map(|t| (t.id.clone(), Runtime::new(t)))
            .collect();
        let bridge = Bridge {
            thermostats,
            sensor_cache: HashMap::new(),
            config_path: config_path.to_string(),
        };
        let handle = Self {
            inner: Arc::new(Mutex::new(bridge)),
            publisher,
            mqtt,
            plugin_id: cfg.homecore.plugin_id.clone(),
            snapshot,
        };
        handle.refresh_snapshot().await;
        Ok(handle)
    }

    /// Rebuild the sync-readable snapshot after any config mutation.
    async fn refresh_snapshot(&self) {
        let list = self.get_thermostats().await;
        if let Ok(mut snap) = self.snapshot.lock() {
            *snap = list;
        }
    }

    /// Register every configured thermostat with HomeCore. Call after
    /// `run_managed` is spawned.
    pub async fn register_all(&self) -> Result<()> {
        let ids: Vec<String> = {
            let b = self.inner.lock().await;
            b.thermostats.values().map(|r| r.device_id()).collect()
        };
        let (names, cfgs): (Vec<String>, Vec<ThermostatEntry>) = {
            let b = self.inner.lock().await;
            b.thermostats
                .values()
                .map(|r| (r.cfg.name.clone(), r.cfg.clone()))
                .unzip()
        };
        for (id, name) in ids.iter().zip(names.iter()) {
            self.publisher
                .register_device_full(id, name, Some("thermostat"), None, None)
                .await?;
            self.publisher.subscribe_commands(id).await?;
            self.publisher.publish_availability(id, true).await?;
        }
        // Also publish initial state for each thermostat.
        for (id, _) in ids.iter().zip(cfgs.iter()) {
            if let Some(payload) = {
                let b = self.inner.lock().await;
                b.thermostats
                    .values()
                    .find(|r| r.device_id() == *id)
                    .map(|r| r.state_payload())
            } {
                self.publisher.publish_state(id, &payload).await?;
            }
        }
        Ok(())
    }

    /// Return the list of device IDs (thermostat_<id>) this plugin owns.
    pub async fn all_own_device_ids(&self) -> Vec<String> {
        let b = self.inner.lock().await;
        b.thermostats.values().map(|r| r.device_id()).collect()
    }

    /// Return the union of all configured sensor IDs across all thermostats.
    pub async fn all_sensor_ids(&self) -> Vec<String> {
        let b = self.inner.lock().await;
        let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
        for r in b.thermostats.values() {
            for id in &r.cfg.sensor_device_ids {
                set.insert(id.clone());
            }
        }
        set.into_iter().collect()
    }

    /// Incoming state message on one of OUR thermostat device topics. Called
    /// at startup via retained-message replay to restore `actuator_last_change`
    /// (and therefore short-cycle lockout windows) across plugin restarts.
    ///
    /// Safe to be called after we've already started publishing fresh state —
    /// we only apply the retained fields if they weren't already overwritten.
    pub async fn on_own_state_restored(&self, device_id: &str, payload: Value) {
        let Some(therm_id) = device_id.strip_prefix("thermostat_") else {
            return;
        };
        let mut b = self.inner.lock().await;
        let Some(rt) = b.thermostats.get_mut(therm_id) else {
            return;
        };
        // Only restore from retained state if we haven't produced live state yet
        // (i.e. actuator_last_change is None). Otherwise we'd clobber the fresh
        // startup recalc.
        if rt.actuator_last_change.is_some() {
            return;
        }
        if let Some(ts) = payload
            .get("actuator_last_change")
            .and_then(|v| v.as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        {
            rt.actuator_last_change = Some(ts.with_timezone(&Utc));
        }
        if let Some(b_) = payload.get("actuator_state").and_then(|v| v.as_bool()) {
            rt.actuator_state = b_;
        }
        if let Some(s) = payload.get("call_for").and_then(|v| v.as_str()) {
            rt.call_for = s.to_string();
        }
        debug!(device_id, "Restored runtime state from retained message");
    }

    /// Incoming state message from an external sensor device.
    pub async fn on_sensor_state(&self, sensor_id: &str, payload: Value) {
        // Extract the configured attribute — the attribute name is per-thermostat,
        // but the common case is "temperature".
        let attrs: Vec<String> = {
            let b = self.inner.lock().await;
            b.thermostats
                .values()
                .filter(|r| r.cfg.sensor_device_ids.iter().any(|s| s == sensor_id))
                .map(|r| r.cfg.sensor_attribute.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        };
        if attrs.is_empty() {
            return; // no thermostat cares about this sensor
        }

        // Pick the first matching numeric attribute. (Most thermostats will share
        // "temperature"; we update the cache under that attribute name.)
        for attr in attrs {
            if let Some(v) = payload.get(&attr).and_then(|v| v.as_f64()) {
                let mut b = self.inner.lock().await;
                b.sensor_cache.insert(sensor_id.to_string(), v);
                debug!(sensor_id, attr = %attr, value = v, "Sensor update cached");
                break;
            }
        }

        // Recalculate every thermostat that depends on this sensor.
        let affected: Vec<String> = {
            let b = self.inner.lock().await;
            b.thermostats
                .values()
                .filter(|r| r.cfg.sensor_device_ids.iter().any(|s| s == sensor_id))
                .map(|r| r.cfg.id.clone())
                .collect()
        };
        for id in affected {
            self.recalculate(&id).await;
        }
    }

    /// Incoming command on this plugin's own device topic.
    pub async fn on_device_command(&self, device_id: &str, payload: Value) {
        let Some(therm_id) = device_id.strip_prefix("thermostat_") else {
            return;
        };
        let cmd = payload.get("command").and_then(|v| v.as_str()).unwrap_or("");
        debug!(device_id, cmd, "Thermostat command");

        // Sensor set changes need a sub/unsub diff against the union of all
        // thermostats' sensor sets AFTER the update. Collect those outside the
        // lock so we can await subscribe_state/unsubscribe_state cleanly.
        let mut changed = false;
        let mut sensor_diff: Option<(Vec<String>, Vec<String>)> = None;
        {
            let mut b = self.inner.lock().await;
            // Snapshot global sensor union BEFORE mutation.
            let old_union: std::collections::HashSet<String> = b
                .thermostats
                .values()
                .flat_map(|r| r.cfg.sensor_device_ids.iter().cloned())
                .collect();

            let Some(rt) = b.thermostats.get_mut(therm_id) else {
                warn!(device_id, "Unknown thermostat");
                return;
            };
            match cmd {
                "set_setpoint" => {
                    if let Some(v) = payload.get("value").and_then(|v| v.as_f64()) {
                        rt.cfg.setpoint = v;
                        changed = true;
                    }
                }
                "set_mode" => {
                    if let Some(m) = payload.get("value").and_then(|v| v.as_str()) {
                        if matches!(m, "heat" | "cool" | "off") {
                            rt.cfg.mode = m.to_string();
                            changed = true;
                        } else {
                            warn!(device_id, mode = %m, "Unknown mode");
                        }
                    }
                }
                "set_hysteresis" => {
                    if let Some(v) = payload.get("value").and_then(|v| v.as_f64()) {
                        rt.cfg.hysteresis = v.max(0.0);
                        changed = true;
                    }
                }
                "set_aggregation" => {
                    if let Some(v) = payload.get("value").and_then(|v| v.as_str()) {
                        if matches!(v, "average" | "min" | "max") {
                            rt.cfg.aggregation = v.to_string();
                            changed = true;
                        }
                    }
                }
                "set_short_cycle" => {
                    if let Some(v) = payload.get("min_on_secs").and_then(|v| v.as_u64()) {
                        rt.cfg.min_on_secs = v;
                        changed = true;
                    }
                    if let Some(v) = payload.get("min_off_secs").and_then(|v| v.as_u64()) {
                        rt.cfg.min_off_secs = v;
                        changed = true;
                    }
                }
                "set_sensors" => {
                    if let Some(ids) = payload.get("sensor_ids").and_then(|v| v.as_array()) {
                        let new_ids: Vec<String> = ids
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect();
                        rt.cfg.sensor_device_ids = new_ids;
                        changed = true;
                    }
                    if let Some(a) = payload.get("attribute").and_then(|v| v.as_str()) {
                        rt.cfg.sensor_attribute = a.to_string();
                        changed = true;
                    }
                }
                "set_actuator" => {
                    if let Some(aid) = payload.get("device_id").and_then(|v| v.as_str()) {
                        rt.cfg.actuator_device_id = aid.to_string();
                        changed = true;
                    }
                    if let Some(v) = payload.get("on_cmd") {
                        rt.cfg.actuator_on_cmd = Some(v.clone());
                        changed = true;
                    }
                    if let Some(v) = payload.get("off_cmd") {
                        rt.cfg.actuator_off_cmd = Some(v.clone());
                        changed = true;
                    }
                }
                "recalculate" | "" => {}
                other => {
                    warn!(device_id, cmd = %other, "Unknown thermostat command");
                    return;
                }
            }

            // Snapshot NEW global sensor union and compute diff.
            let new_union: std::collections::HashSet<String> = b
                .thermostats
                .values()
                .flat_map(|r| r.cfg.sensor_device_ids.iter().cloned())
                .collect();
            if old_union != new_union {
                let added: Vec<String> = new_union.difference(&old_union).cloned().collect();
                let removed: Vec<String> = old_union.difference(&new_union).cloned().collect();
                sensor_diff = Some((added, removed));
            }
        }

        // Apply subscription diff outside the lock.
        if let Some((added, removed)) = sensor_diff {
            for sid in &added {
                if let Err(e) = self.publisher.subscribe_state(sid).await {
                    warn!(sensor = %sid, error = %e, "set_sensors: subscribe failed");
                }
            }
            for sid in &removed {
                if let Err(e) = self.publisher.unsubscribe_state(sid).await {
                    warn!(sensor = %sid, error = %e, "set_sensors: unsubscribe failed");
                }
            }
        }

        if changed {
            if let Err(e) = self.persist_config().await {
                warn!(error = %e, "Failed to persist config");
            }
        }
        self.recalculate(therm_id).await;
    }

    /// Recalculate a single thermostat and publish updated state / actuator cmd.
    pub async fn recalculate(&self, therm_id: &str) {
        let (publish_cmd, device_id, state_payload) = {
            let mut b = self.inner.lock().await;
            let cache = b.sensor_cache.clone();
            let Some(rt) = b.thermostats.get_mut(therm_id) else {
                return;
            };

            // 1. Gather readings from cache.
            let mut readings: Vec<f64> = Vec::with_capacity(rt.cfg.sensor_device_ids.len());
            for sid in &rt.cfg.sensor_device_ids {
                if let Some(v) = cache.get(sid) {
                    readings.push(*v);
                }
            }

            // 2. Stale path.
            if readings.is_empty() && rt.cfg.mode != "off" {
                if rt.call_for != "stale" {
                    warn!(id = %rt.cfg.id, "No sensor readings available");
                    rt.call_for = "stale".into();
                    rt.current_temperature = None;
                }
                (None, rt.device_id(), rt.state_payload())
            } else {
                let current = aggregate(&readings, &rt.cfg.aggregation);

                // 3. Desired call_for.
                let new_call: &str = if rt.cfg.mode == "off" {
                    "idle"
                } else {
                    match current {
                        Some(t) => compute_call_for(
                            t,
                            rt.cfg.setpoint,
                            rt.cfg.hysteresis,
                            &rt.cfg.mode,
                            &rt.call_for,
                        ),
                        None => "idle",
                    }
                };
                let desired_act = new_call == "heat" || new_call == "cool";

                // 4. Lockout check.
                let now = Utc::now();
                let force_off = rt.cfg.mode == "off" && rt.actuator_state;
                let remaining = if force_off {
                    0
                } else if desired_act != rt.actuator_state {
                    lockout_remaining(
                        rt.actuator_state,
                        rt.cfg.min_on_secs,
                        rt.cfg.min_off_secs,
                        rt.actuator_last_change,
                        now,
                    )
                } else {
                    0
                };

                let publish_cmd: Option<(String, Value)> = if remaining > 0 {
                    rt.pending_call = Some(new_call.to_string());
                    rt.lockout_until = Some(now + chrono::Duration::seconds(remaining as i64));
                    rt.current_temperature = current;
                    rt.call_for = new_call.to_string();
                    None
                } else if desired_act != rt.actuator_state {
                    let payload = if desired_act {
                        rt.cfg
                            .actuator_on_cmd
                            .clone()
                            .unwrap_or_else(|| json!({"command": "on"}))
                    } else {
                        rt.cfg
                            .actuator_off_cmd
                            .clone()
                            .unwrap_or_else(|| json!({"command": "off"}))
                    };
                    rt.actuator_state = desired_act;
                    rt.actuator_last_change = Some(now);
                    rt.pending_call = None;
                    rt.lockout_until = None;
                    rt.current_temperature = current;
                    rt.call_for = new_call.to_string();
                    if rt.cfg.actuator_device_id.is_empty() {
                        None
                    } else {
                        Some((rt.cfg.actuator_device_id.clone(), payload))
                    }
                } else {
                    rt.pending_call = None;
                    rt.lockout_until = None;
                    rt.current_temperature = current;
                    rt.call_for = new_call.to_string();
                    None
                };

                (publish_cmd, rt.device_id(), rt.state_payload())
            }
        };

        // 5. Publish updated state.
        if let Err(e) = self.publisher.publish_state(&device_id, &state_payload).await {
            warn!(device_id, error = %e, "Failed to publish thermostat state");
        }

        // 6. Issue actuator command, if any.
        if let Some((actuator_id, payload)) = publish_cmd {
            let change = DeviceChange::homecore("thermostat");
            let payload = with_command_change_metadata(payload, &change);
            let topic = format!("homecore/devices/{actuator_id}/cmd");
            let publish_err: Option<String> = match serde_json::to_vec(&payload) {
                Ok(bytes) => {
                    match self
                        .mqtt
                        .publish(&topic, QoS::AtMostOnce, false, bytes)
                        .await
                    {
                        Ok(()) => {
                            info!(
                                device_id = %device_id,
                                actuator = %actuator_id,
                                "Thermostat published actuator command"
                            );
                            None
                        }
                        Err(e) => {
                            warn!(actuator_id, error = %e, "Failed to publish actuator command");
                            Some(e.to_string())
                        }
                    }
                }
                Err(e) => {
                    warn!(actuator_id, error = %e, "Failed to serialise actuator payload");
                    Some(e.to_string())
                }
            };

            // Record the outcome on the thermostat's runtime state + re-publish
            // device state so the UI sees it.
            let therm_id = device_id.strip_prefix("thermostat_").unwrap_or("").to_string();
            if !therm_id.is_empty() {
                let updated_payload = {
                    let mut b = self.inner.lock().await;
                    if let Some(rt) = b.thermostats.get_mut(&therm_id) {
                        rt.actuator_last_error = publish_err.map(|m| ActuatorError {
                            timestamp: Utc::now(),
                            message: m,
                        });
                        Some(rt.state_payload())
                    } else {
                        None
                    }
                };
                if let Some(p) = updated_payload {
                    let _ = self.publisher.publish_state(&device_id, &p).await;
                }
            }
        }
    }

    /// Recalculate every thermostat (startup reconciliation and lockout retry).
    pub async fn recalculate_all(&self) {
        let ids: Vec<String> = {
            let b = self.inner.lock().await;
            b.thermostats.keys().cloned().collect()
        };
        for id in ids {
            self.recalculate(&id).await;
        }
    }

    /// Tick — recalculate any thermostats that have a pending_call (i.e. a
    /// short-cycle lockout deferred an actuator command).
    pub async fn tick(&self) {
        let ids: Vec<String> = {
            let b = self.inner.lock().await;
            b.thermostats
                .values()
                .filter(|r| r.pending_call.is_some())
                .map(|r| r.cfg.id.clone())
                .collect()
        };
        for id in ids {
            self.recalculate(&id).await;
        }
    }

    /// Write current in-memory config back to disk so restart is idempotent.
    /// Also refreshes the sync-readable thermostat snapshot.
    async fn persist_config(&self) -> Result<()> {
        let cfg = self.assemble_config().await;
        let toml_str = toml::to_string_pretty(&cfg)?;
        let path = {
            let b = self.inner.lock().await;
            b.config_path.clone()
        };
        // Atomic write: temp + rename
        let tmp = format!("{path}.partial");
        std::fs::write(&tmp, toml_str)?;
        std::fs::rename(&tmp, &path)?;
        debug!(path, "Persisted config to disk");
        self.refresh_snapshot().await;
        Ok(())
    }

    /// Rebuild a Config from current in-memory state (for persistence).
    async fn assemble_config(&self) -> Config {
        // Reload from disk to preserve [plugin] / [logging] sections that aren't
        // owned by runtime, then overwrite [[thermostat]] entries from memory.
        let b = self.inner.lock().await;
        let mut cfg = Config::load(&b.config_path).unwrap_or_else(|_| Config {
            homecore: crate::config::HomecoreSection {
                plugin_id: self.plugin_id.clone(),
                broker_host: "127.0.0.1".into(),
                broker_port: 1883,
                password: String::new(),
                heartbeat_secs: 60,
            },
            logging: Default::default(),
            thermostats: vec![],
        });
        cfg.thermostats = b.thermostats.values().map(|r| r.cfg.clone()).collect();
        cfg.thermostats.sort_by(|a, b| a.id.cmp(&b.id));
        cfg
    }

    /// Add a new thermostat from a runtime command (UI wizard). Persists to
    /// config.toml, registers the device, subscribes any new sensors, and
    /// triggers an initial recalculation.
    pub async fn add_thermostat(&self, entry: ThermostatEntry) -> Result<String> {
        // Validate mode + aggregation before doing anything irreversible.
        if !matches!(entry.mode.as_str(), "heat" | "cool" | "off") {
            return Err(anyhow::anyhow!("invalid mode: {}", entry.mode));
        }
        if !matches!(entry.aggregation.as_str(), "average" | "min" | "max") {
            return Err(anyhow::anyhow!("invalid aggregation: {}", entry.aggregation));
        }
        if entry.hysteresis < 0.0 {
            return Err(anyhow::anyhow!("hysteresis must be non-negative"));
        }

        let therm_id = entry.id.clone();
        let device_id = format!("thermostat_{therm_id}");
        let display_name = entry.name.clone();

        let sensor_added: Vec<String>;
        {
            let mut b = self.inner.lock().await;
            if b.thermostats.contains_key(&therm_id) {
                return Err(anyhow::anyhow!("thermostat already exists: {therm_id}"));
            }
            let old_union: std::collections::HashSet<String> = b
                .thermostats
                .values()
                .flat_map(|r| r.cfg.sensor_device_ids.iter().cloned())
                .collect();
            b.thermostats.insert(therm_id.clone(), Runtime::new(entry));
            let new_union: std::collections::HashSet<String> = b
                .thermostats
                .values()
                .flat_map(|r| r.cfg.sensor_device_ids.iter().cloned())
                .collect();
            sensor_added = new_union.difference(&old_union).cloned().collect();
        }

        // Subscribe to new sensors.
        for sid in &sensor_added {
            if let Err(e) = self.publisher.subscribe_state(sid).await {
                warn!(sensor = %sid, error = %e, "add_thermostat: subscribe failed");
            }
        }

        // Register the new device.
        self.publisher
            .register_device_full(&device_id, &display_name, Some("thermostat"), None, None)
            .await?;
        self.publisher.subscribe_commands(&device_id).await?;
        self.publisher.publish_availability(&device_id, true).await?;

        // Persist + initial recalc.
        self.persist_config().await?;
        self.recalculate(&therm_id).await;
        Ok(therm_id)
    }

    /// Remove a thermostat. Unsubscribes orphan sensors (sensors no longer
    /// referenced by any other thermostat), drops the device, persists config.
    pub async fn remove_thermostat(&self, therm_id: &str) -> Result<()> {
        let device_id = format!("thermostat_{therm_id}");
        let sensors_removed: Vec<String>;
        {
            let mut b = self.inner.lock().await;
            if !b.thermostats.contains_key(therm_id) {
                return Err(anyhow::anyhow!("thermostat not found: {therm_id}"));
            }
            let removed_sensors: std::collections::HashSet<String> = b
                .thermostats
                .get(therm_id)
                .unwrap()
                .cfg
                .sensor_device_ids
                .iter()
                .cloned()
                .collect();
            b.thermostats.remove(therm_id);
            // Any sensor no longer used by remaining thermostats is orphan.
            let still_used: std::collections::HashSet<String> = b
                .thermostats
                .values()
                .flat_map(|r| r.cfg.sensor_device_ids.iter().cloned())
                .collect();
            sensors_removed = removed_sensors.difference(&still_used).cloned().collect();
        }

        for sid in &sensors_removed {
            if let Err(e) = self.publisher.unsubscribe_state(sid).await {
                warn!(sensor = %sid, error = %e, "remove_thermostat: unsubscribe failed");
            }
        }

        // Mark device offline + clear retained state.
        let _ = self.publisher.publish_availability(&device_id, false).await;
        let topic = format!("homecore/devices/{device_id}/state");
        let _ = self
            .mqtt
            .publish(&topic, QoS::AtLeastOnce, true, Vec::<u8>::new())
            .await;

        self.persist_config().await?;
        info!(thermostat = %therm_id, "Removed thermostat");
        Ok(())
    }

    /// Return the current list of thermostat configs as JSON.
    pub async fn get_thermostats(&self) -> Vec<Value> {
        let b = self.inner.lock().await;
        let mut out: Vec<_> = b
            .thermostats
            .values()
            .map(|r| serde_json::to_value(&r.cfg).unwrap_or(Value::Null))
            .collect();
        out.sort_by(|a, b| {
            a.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .cmp(b.get("id").and_then(|v| v.as_str()).unwrap_or(""))
        });
        out
    }

    /// Reload config from disk. Applies changes to in-memory thermostats and
    /// adjusts MQTT subscriptions to match the new sensor set.
    pub async fn reload_config(&self) -> Result<()> {
        let path = {
            let b = self.inner.lock().await;
            b.config_path.clone()
        };
        let new_cfg = Config::load(&path)?;
        let added_sensors: Vec<String>;
        let removed_sensors: Vec<String>;

        {
            let mut b = self.inner.lock().await;
            let old_set: std::collections::HashSet<String> = b
                .thermostats
                .values()
                .flat_map(|r| r.cfg.sensor_device_ids.iter().cloned())
                .collect();
            let new_set: std::collections::HashSet<String> = new_cfg
                .thermostats
                .iter()
                .flat_map(|t| t.sensor_device_ids.iter().cloned())
                .collect();
            added_sensors = new_set.difference(&old_set).cloned().collect();
            removed_sensors = old_set.difference(&new_set).cloned().collect();

            // Upsert thermostat configs. Preserve runtime state when the
            // thermostat id already exists.
            for entry in new_cfg.thermostats {
                b.thermostats
                    .entry(entry.id.clone())
                    .and_modify(|rt| rt.cfg = entry.clone())
                    .or_insert_with(|| Runtime::new(entry));
            }
        }

        // Apply subscription diff via the publisher so the shared
        // SubscriptionTracker is updated — subscriptions survive reconnects.
        for sid in &added_sensors {
            if let Err(e) = self.publisher.subscribe_state(sid).await {
                warn!(sensor = %sid, error = %e, "Reload: subscribe_state failed");
            }
        }
        for sid in &removed_sensors {
            if let Err(e) = self.publisher.unsubscribe_state(sid).await {
                warn!(sensor = %sid, error = %e, "Reload: unsubscribe_state failed");
            }
        }

        info!(
            added = added_sensors.len(),
            removed = removed_sensors.len(),
            "Config reloaded"
        );
        self.refresh_snapshot().await;
        Ok(())
    }
}

// ── Helper: build the per-plugin bridge command channel ──────────────────────

/// Open a one-shot channel used by management handlers to trigger bridge
/// actions (recalculate_all, reload_config) from synchronous callback context.
pub enum BridgeTask {
    RecalculateAll,
    ReloadConfig,
    AddThermostat {
        entry: Box<ThermostatEntry>,
    },
    RemoveThermostat {
        id: String,
    },
}

pub fn bridge_task_channel() -> (mpsc::Sender<BridgeTask>, mpsc::Receiver<BridgeTask>) {
    mpsc::channel(16)
}
