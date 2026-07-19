use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::api::schema::{
    ControlClientAccessMode, ControlClientStatus, EventData, EventEnvelope, EventKind,
};

use super::App;

pub(crate) const CONTROL_CLIENT_LEASE_DURATION: Duration = Duration::from_secs(15);
const MAX_CONTROL_CLIENTS: usize = 64;
const MAX_CONTROL_CLIENT_ID_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlClientRegistryError {
    InvalidClientId,
    AtCapacity,
    NotFound,
}

impl ControlClientRegistryError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::InvalidClientId => "invalid_client_id",
            Self::AtCapacity => "too_many_control_clients",
            Self::NotFound => "control_client_not_found",
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::InvalidClientId => "client_id must contain between 1 and 128 bytes",
            Self::AtCapacity => "no more than 64 control clients may be registered",
            Self::NotFound => "control client lease was not found or has expired",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ControlClientLease {
    access_mode: ControlClientAccessMode,
    expires_at: Instant,
}

#[derive(Default)]
pub(crate) struct ControlClientRegistry {
    leases: HashMap<String, ControlClientLease>,
}

impl ControlClientRegistry {
    fn validate_client_id(client_id: &str) -> Result<(), ControlClientRegistryError> {
        if client_id.is_empty() || client_id.len() > MAX_CONTROL_CLIENT_ID_BYTES {
            return Err(ControlClientRegistryError::InvalidClientId);
        }
        Ok(())
    }

    pub(crate) fn register(
        &mut self,
        client_id: String,
        access_mode: ControlClientAccessMode,
        now: Instant,
    ) -> Result<(), ControlClientRegistryError> {
        Self::validate_client_id(&client_id)?;
        self.expire(now);
        if !self.leases.contains_key(&client_id) && self.leases.len() >= MAX_CONTROL_CLIENTS {
            return Err(ControlClientRegistryError::AtCapacity);
        }
        self.leases.insert(
            client_id,
            ControlClientLease {
                access_mode,
                expires_at: now + CONTROL_CLIENT_LEASE_DURATION,
            },
        );
        Ok(())
    }

    pub(crate) fn heartbeat(
        &mut self,
        client_id: &str,
        now: Instant,
    ) -> Result<(), ControlClientRegistryError> {
        Self::validate_client_id(client_id)?;
        let Some(lease) = self.leases.get(client_id).copied() else {
            return Err(ControlClientRegistryError::NotFound);
        };
        if lease.expires_at <= now {
            self.leases.remove(client_id);
            return Err(ControlClientRegistryError::NotFound);
        }
        if let Some(lease) = self.leases.get_mut(client_id) {
            lease.expires_at = now + CONTROL_CLIENT_LEASE_DURATION;
        }
        Ok(())
    }

    pub(crate) fn unregister(&mut self, client_id: &str) -> Result<(), ControlClientRegistryError> {
        Self::validate_client_id(client_id)?;
        self.leases.remove(client_id);
        Ok(())
    }

    pub(crate) fn expire(&mut self, now: Instant) {
        self.leases.retain(|_, lease| lease.expires_at > now);
    }

    pub(crate) fn status(&self) -> ControlClientStatus {
        let mut status = ControlClientStatus::default();
        for lease in self.leases.values() {
            status.total_count = status.total_count.saturating_add(1);
            match lease.access_mode {
                ControlClientAccessMode::Restricted => {
                    status.restricted_count = status.restricted_count.saturating_add(1);
                }
                ControlClientAccessMode::FullControl => {
                    status.full_control_count = status.full_control_count.saturating_add(1);
                }
            }
        }
        status
    }
}

impl App {
    pub(super) fn register_control_client(
        &mut self,
        client_id: String,
        access_mode: ControlClientAccessMode,
        now: Instant,
    ) -> Result<ControlClientStatus, ControlClientRegistryError> {
        let result = self.control_clients.register(client_id, access_mode, now);
        self.sync_control_client_status();
        result.map(|()| self.state.control_client_status)
    }

    pub(super) fn heartbeat_control_client(
        &mut self,
        client_id: &str,
        now: Instant,
    ) -> Result<ControlClientStatus, ControlClientRegistryError> {
        let result = self.control_clients.heartbeat(client_id, now);
        self.sync_control_client_status();
        result.map(|()| self.state.control_client_status)
    }

    pub(super) fn unregister_control_client(
        &mut self,
        client_id: &str,
    ) -> Result<ControlClientStatus, ControlClientRegistryError> {
        let result = self.control_clients.unregister(client_id);
        self.sync_control_client_status();
        result.map(|()| self.state.control_client_status)
    }

    pub(super) fn control_client_status(&self) -> ControlClientStatus {
        self.state.control_client_status
    }

    pub(crate) fn expire_control_client_leases(&mut self, now: Instant) -> bool {
        self.control_clients.expire(now);
        self.sync_control_client_status()
    }

    fn sync_control_client_status(&mut self) -> bool {
        let status = self.control_clients.status();
        if status == self.state.control_client_status {
            return false;
        }
        self.state.control_client_status = status;
        self.emit_event(EventEnvelope {
            event: EventKind::ControlClientPresenceChanged,
            data: EventData::ControlClientPresenceChanged { status },
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_expire_independently_from_their_registration_time() {
        let start = Instant::now();
        let mut registry = ControlClientRegistry::default();
        registry
            .register("first".into(), ControlClientAccessMode::Restricted, start)
            .unwrap();
        registry
            .register(
                "second".into(),
                ControlClientAccessMode::FullControl,
                start + Duration::from_secs(5),
            )
            .unwrap();

        registry.expire(start + CONTROL_CLIENT_LEASE_DURATION);
        assert_eq!(
            registry.status(),
            ControlClientStatus {
                total_count: 1,
                restricted_count: 0,
                full_control_count: 1,
            }
        );
        registry.expire(start + CONTROL_CLIENT_LEASE_DURATION + Duration::from_secs(5));
        assert_eq!(registry.status(), ControlClientStatus::default());
    }

    #[test]
    fn heartbeat_renews_only_an_existing_unexpired_lease() {
        let start = Instant::now();
        let mut registry = ControlClientRegistry::default();
        registry
            .register("bridge".into(), ControlClientAccessMode::Restricted, start)
            .unwrap();
        registry
            .heartbeat("bridge", start + Duration::from_secs(10))
            .unwrap();
        registry.expire(start + Duration::from_secs(16));
        assert_eq!(registry.status().total_count, 1);
        assert_eq!(
            registry.heartbeat("missing", start),
            Err(ControlClientRegistryError::NotFound)
        );
        assert_eq!(
            registry.heartbeat("bridge", start + Duration::from_secs(25)),
            Err(ControlClientRegistryError::NotFound)
        );
    }

    #[test]
    fn registration_is_idempotent_and_can_change_access_mode() {
        let start = Instant::now();
        let mut registry = ControlClientRegistry::default();
        registry
            .register("bridge".into(), ControlClientAccessMode::Restricted, start)
            .unwrap();
        registry
            .register(
                "bridge".into(),
                ControlClientAccessMode::FullControl,
                start + Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(
            registry.status(),
            ControlClientStatus {
                total_count: 1,
                restricted_count: 0,
                full_control_count: 1,
            }
        );
        registry.unregister("bridge").unwrap();
        registry.unregister("bridge").unwrap();
        assert_eq!(registry.status(), ControlClientStatus::default());
    }

    #[test]
    fn registry_rejects_invalid_ids_and_more_than_sixty_four_clients() {
        let start = Instant::now();
        let mut registry = ControlClientRegistry::default();
        assert_eq!(
            registry.register("".into(), ControlClientAccessMode::Restricted, start),
            Err(ControlClientRegistryError::InvalidClientId)
        );
        for index in 0..MAX_CONTROL_CLIENTS {
            registry
                .register(
                    format!("bridge-{index}"),
                    ControlClientAccessMode::Restricted,
                    start,
                )
                .unwrap();
        }
        assert_eq!(
            registry.register(
                "overflow".into(),
                ControlClientAccessMode::Restricted,
                start,
            ),
            Err(ControlClientRegistryError::AtCapacity)
        );
    }
}
