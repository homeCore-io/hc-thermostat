//! hc-thermostat — virtual thermostat plugin for homeCore.

use anyhow::Result;
use plugin_sdk_rs::{PluginClient, PluginConfig};
use std::sync::Arc;
use tracing::{error, info, warn};

mod bridge;
mod config;
mod control;
mod logging;

use bridge::{BridgeHandle, BridgeTask};
use config::Config;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() {
    // Accept either `hc-thermostat --config PATH` or `hc-thermostat PATH`.
    // Default to `config/config.toml` (relative to the working directory).
    let config_path = parse_flag("--config")
        .or_else(|| {
            std::env::args()
                .nth(1)
                .filter(|a| !a.starts_with("--"))
        })
        .unwrap_or_else(|| "config/config.toml".to_string());

    // Load [logging] bootstrap first so file logging is up before config
    // parse errors (otherwise a bad config silently goes only to stderr).
    let (_log_guard, log_level_handle, mqtt_log_handle) = init_logging_bootstrap(&config_path);

    let cfg = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, path = %config_path, "Failed to load config");
            std::process::exit(1);
        }
    };

    info!(version = VERSION, plugin_id = %cfg.homecore.plugin_id, "Starting hc-thermostat");

    if let Err(e) = run(cfg, config_path, log_level_handle, mqtt_log_handle).await {
        error!(error = %e, "Plugin exited with error");
        std::process::exit(1);
    }
}

/// Read `[logging]` from the config file (best-effort, tolerating errors) so
/// tracing is initialized before we strictly validate the full config.
fn init_logging_bootstrap(
    config_path: &str,
) -> (
    tracing_appender::non_blocking::WorkerGuard,
    hc_logging::LogLevelHandle,
    plugin_sdk_rs::mqtt_log_layer::MqttLogHandle,
) {
    #[derive(serde::Deserialize, Default)]
    struct Bootstrap {
        #[serde(default)]
        logging: logging::LoggingConfig,
    }
    let bootstrap: Bootstrap = std::fs::read_to_string(config_path)
        .ok()
        .and_then(|s| toml::from_str(&s).ok())
        .unwrap_or_default();
    logging::init_logging(
        config_path,
        "hc-thermostat",
        "hc_thermostat=info",
        &bootstrap.logging,
    )
}

/// Extract `--flag value` from argv for an arbitrary flag name. Returns the
/// value if the flag was provided.
fn parse_flag(flag: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let idx = args.iter().position(|a| a == flag)?;
    args.get(idx + 1).cloned()
}

async fn run(
    cfg: Config,
    config_path: String,
    log_level_handle: hc_logging::LogLevelHandle,
    mqtt_log_handle: plugin_sdk_rs::mqtt_log_layer::MqttLogHandle,
) -> Result<()> {
    // 1. Connect to broker.
    let sdk_config = PluginConfig {
        broker_host: cfg.homecore.broker_host.clone(),
        broker_port: cfg.homecore.broker_port,
        plugin_id: cfg.homecore.plugin_id.clone(),
        password: cfg.homecore.password.clone(),
    };
    let client = PluginClient::connect(sdk_config).await?;

    // 2. Activate MQTT log forwarding now that we're connected.
    mqtt_log_handle.connect(
        client.mqtt_client(),
        &cfg.homecore.plugin_id,
        &cfg.logging.log_forward_level,
    );

    // 3. Bridge task channel — used by management custom handlers to poke the
    //    control loop from the synchronous callback context.
    let (bridge_tx, mut bridge_rx) = bridge::bridge_task_channel();

    // 3b. Sync-readable thermostat snapshot. The bridge writes into it after
    // any config mutation; the SDK's sync custom command handler reads from
    // it when answering `get_thermostats`. Shared Arc lets both sides point
    // at the same Vec.
    let snapshot: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    // 4. Management: enable heartbeat + config/log protocol + custom commands.
    //
    // The SDK calls custom handlers SYNCHRONOUSLY inside the MQTT event loop.
    // Any blocking here stalls publishes (including the management response)
    // and causes the API caller to time out. So:
    //   - Commands that just trigger async work → try_send + return "queued"
    //   - Commands that need to return data (get_thermostats) → read from
    //     the bridge's sync-readable snapshot (std::sync::Mutex)
    let bridge_tx_for_mgmt = bridge_tx.clone();
    let snapshot_handle = snapshot.clone();
    let mgmt = client
        .enable_management(
            cfg.homecore.heartbeat_secs,
            Some(VERSION.to_string()),
            Some(config_path.clone()),
            Some(log_level_handle),
        )
        .await?
        .with_custom_handler(move |cmd| match cmd["action"].as_str()? {
            "recalculate_all" => {
                let _ = bridge_tx_for_mgmt.try_send(BridgeTask::RecalculateAll);
                Some(serde_json::json!({ "status": "ok" }))
            }
            "reload_config" => {
                let _ = bridge_tx_for_mgmt.try_send(BridgeTask::ReloadConfig);
                Some(serde_json::json!({ "status": "ok" }))
            }
            "add_thermostat" => {
                let entry_val = cmd.get("config")?.clone();
                let entry: config::ThermostatEntry = match serde_json::from_value(entry_val) {
                    Ok(e) => e,
                    Err(e) => {
                        return Some(serde_json::json!({
                            "status": "error",
                            "error": format!("invalid thermostat config: {e}"),
                        }));
                    }
                };
                let id = entry.id.clone();
                if bridge_tx_for_mgmt
                    .try_send(BridgeTask::AddThermostat { entry: Box::new(entry) })
                    .is_err()
                {
                    return Some(serde_json::json!({
                        "status": "error",
                        "error": "bridge queue full",
                    }));
                }
                Some(serde_json::json!({ "status": "queued", "id": id }))
            }
            "remove_thermostat" => {
                let Some(id) = cmd.get("id").and_then(|v| v.as_str()) else {
                    return Some(serde_json::json!({
                        "status": "error",
                        "error": "missing `id`",
                    }));
                };
                if bridge_tx_for_mgmt
                    .try_send(BridgeTask::RemoveThermostat { id: id.to_string() })
                    .is_err()
                {
                    return Some(serde_json::json!({
                        "status": "error",
                        "error": "bridge queue full",
                    }));
                }
                Some(serde_json::json!({ "status": "queued", "id": id }))
            }
            "get_thermostats" => {
                // Sync read from the shadow snapshot — no bridge round-trip.
                let list = snapshot_handle
                    .lock()
                    .ok()
                    .map(|g| g.clone())
                    .unwrap_or_default();
                Some(serde_json::json!({ "status": "ok", "thermostats": list }))
            }
            _ => None,
        });

    // 5. Create bridge + device publisher handle.
    let publisher = client.device_publisher();
    let mqtt_client = client.mqtt_client();
    let bridge = BridgeHandle::new(
        &cfg,
        publisher.clone(),
        mqtt_client,
        &config_path,
        snapshot.clone(),
    )
    .await?;
    let bridge = Arc::new(bridge);

    // 6. Subscribe to external sensor state topics BEFORE spawning run_managed
    //    so the initial retained state is delivered once the loop starts.
    let sensor_ids = bridge.all_sensor_ids().await;
    for sid in &sensor_ids {
        if let Err(e) = client.subscribe_state(sid).await {
            warn!(sensor = %sid, error = %e, "Failed to subscribe to sensor state");
        }
    }
    info!(sensor_count = sensor_ids.len(), "Subscribed to sensor state topics");

    // 6b. Subscribe to OUR OWN thermostat state topics so retained state from
    // the previous run is replayed — restores actuator_last_change across
    // restarts (for correct short-cycle lockout).
    let own_ids = bridge.all_own_device_ids().await;
    for id in &own_ids {
        if let Err(e) = client.subscribe_state(id).await {
            warn!(device_id = %id, error = %e, "Failed to subscribe to own state");
        }
    }

    // 7. Spawn run_managed BEFORE registering devices (critical SDK invariant).
    let bridge_for_cmd = Arc::clone(&bridge);
    let bridge_for_state = Arc::clone(&bridge);

    // State callback routes to the own-state restore path for our thermostat
    // device IDs; everything else is an external sensor.
    let cmd_cb = move |device_id: String, payload: serde_json::Value| {
        let b = Arc::clone(&bridge_for_cmd);
        tokio::spawn(async move {
            b.on_device_command(&device_id, payload).await;
        });
    };
    let state_cb = move |source_id: String, payload: serde_json::Value| {
        let b = Arc::clone(&bridge_for_state);
        tokio::spawn(async move {
            if source_id.starts_with("thermostat_") {
                b.on_own_state_restored(&source_id, payload).await;
            } else {
                b.on_sensor_state(&source_id, payload).await;
            }
        });
    };

    let run_handle = tokio::spawn(async move {
        if let Err(e) = client.run_managed_with_state(cmd_cb, state_cb, mgmt).await {
            error!(error = %e, "PluginClient run loop exited");
        }
    });

    // 8. Settle — let MQTT connect + subscriptions establish + retained
    //    messages arrive before we run the first recalc.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 9. Register devices.
    bridge.register_all().await?;

    // 10. Startup reconciliation — every thermostat recalculates once, now
    //     with actuator_last_change restored from retained state (if any).
    bridge.recalculate_all().await;

    // 11. Main supervisor loop: lockout retry tick + bridge-task dispatch.
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                bridge.tick().await;
            }
            task = bridge_rx.recv() => {
                match task {
                    Some(BridgeTask::RecalculateAll) => {
                        info!("Management: recalculate_all");
                        bridge.recalculate_all().await;
                    }
                    Some(BridgeTask::ReloadConfig) => {
                        info!("Management: reload_config");
                        match bridge.reload_config().await {
                            Ok(()) => {
                                // Register any newly-configured thermostats.
                                if let Err(e) = bridge.register_all().await {
                                    warn!(error = %e, "register_all after reload failed");
                                }
                                bridge.recalculate_all().await;
                            }
                            Err(e) => error!(error = %e, "reload_config failed"),
                        }
                    }
                    Some(BridgeTask::AddThermostat { entry }) => {
                        info!(id = %entry.id, "Management: add_thermostat");
                        if let Err(e) = bridge.add_thermostat(*entry).await {
                            warn!(error = %e, "add_thermostat failed");
                        }
                    }
                    Some(BridgeTask::RemoveThermostat { id }) => {
                        info!(id, "Management: remove_thermostat");
                        if let Err(e) = bridge.remove_thermostat(&id).await {
                            warn!(error = %e, "remove_thermostat failed");
                        }
                    }
                    None => break,
                }
            }
        }
    }

    run_handle.abort();
    Ok(())
}
