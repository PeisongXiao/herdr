use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct PingParams {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServerLiveHandoffParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_exe: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_protocol: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServerCapabilities {
    pub live_handoff: bool,
    #[serde(default)]
    pub detached_server_daemon: bool,
    #[serde(default)]
    pub peer_federation: bool,
    #[serde(default)]
    pub remote_presentation: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_federation_defaults_to_false_when_absent() {
        let capabilities: ServerCapabilities =
            serde_json::from_str(r#"{"live_handoff":false}"#).unwrap();

        assert!(!capabilities.peer_federation);
        assert!(!capabilities.remote_presentation);
    }
}
