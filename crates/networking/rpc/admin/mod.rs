use ethrex_common::types::ChainConfig;
use ethrex_storage::Store;
use serde_json::Value;
use tracing_subscriber::{EnvFilter, Registry, reload};

use crate::{
    rpc::NodeData,
    utils::{RpcErr, RpcRequest},
};
mod peers;
pub use peers::{add_peer, peers};

/// Serialize a ChainConfig to a serde_json::Value, working around the fact
/// that serde_json::to_value cannot represent u128 values exceeding u64::MAX
/// (the terminal_total_difficulty field is u128).
fn chain_config_to_value(config: &ChainConfig) -> Result<Value, RpcErr> {
    let ttd = config.terminal_total_difficulty;

    // Serialize with TTD cleared to avoid u128 overflow in to_value
    let mut config = config.clone();
    config.terminal_total_difficulty = None;

    let mut value =
        serde_json::to_value(&config).map_err(|e| RpcErr::Internal(e.to_string()))?;

    // Re-insert TTD, using u64 when possible, decimal string otherwise
    if let Some(obj) = value.as_object_mut() {
        let ttd_value = match ttd {
            Some(v) => match u64::try_from(v) {
                Ok(n) => Value::Number(n.into()),
                Err(_) => Value::String(v.to_string()),
            },
            None => Value::Null,
        };
        obj.insert("terminalTotalDifficulty".to_string(), ttd_value);
    }

    Ok(value)
}

pub fn node_info(storage: Store, node_data: &NodeData) -> Result<Value, RpcErr> {
    let enode_url = node_data.local_p2p_node.enode_url();
    let enr_url = match node_data.local_node_record.enr_url() {
        Ok(enr) => enr,
        Err(_) => "".into(),
    };

    let chain_config = storage.get_chain_config();
    let chain_config_value = chain_config_to_value(&chain_config)?;

    let mut protocols = serde_json::Map::new();
    protocols.insert("eth".to_string(), chain_config_value);

    Ok(serde_json::json!({
        "enode": enode_url,
        "enr": enr_url,
        "id": hex::encode(node_data.local_p2p_node.node_id()),
        "name": node_data.client_version.to_string(),
        "ip": node_data.local_p2p_node.ip.to_string(),
        "ports": {
            "discovery": node_data.local_p2p_node.udp_port,
            "listener": node_data.local_p2p_node.tcp_port,
        },
        "protocols": Value::Object(protocols),
    }))
}

pub fn set_log_level(
    req: &RpcRequest,
    log_filter_handler: &Option<reload::Handle<EnvFilter, Registry>>,
) -> Result<Value, RpcErr> {
    let params = req
        .params
        .clone()
        .ok_or(RpcErr::MissingParam("log level".to_string()))?;
    let log_level = params
        .first()
        .ok_or(RpcErr::MissingParam("log level".to_string()))?
        .as_str()
        .ok_or(RpcErr::WrongParam("Expected string".to_string()))?;

    let filter = EnvFilter::try_new(log_level)
        .map_err(|_| RpcErr::BadParams(format!("Cannot parse {log_level} as a log directive")))?;

    if let Some(handle) = log_filter_handler {
        handle
            .reload(filter)
            .map_err(|e| RpcErr::Internal(format!("Failed to reload log filter: {}", e)))?;
        Ok(Value::Bool(true))
    } else {
        Err(RpcErr::Internal(
            "Log filter handler not available".to_string(),
        ))
    }
}
