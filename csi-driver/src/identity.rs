//! CSI Identity Service Implementation
//!
//! Provides plugin identification and capability reporting to Kubernetes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::{Request, Response, Status};

use crate::csi;

pub const DRIVER_NAME: &str = "csi.freebsd.org";
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared readiness state for the CSI driver
///
/// Used by the probe() method to report actual readiness status
/// and can be updated by signal handlers during shutdown.
#[derive(Debug)]
pub struct ReadinessState {
    ready: AtomicBool,
}

impl ReadinessState {
    pub fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }
}

impl Default for ReadinessState {
    fn default() -> Self {
        Self::new()
    }
}

/// CSI Identity Service
///
/// Implements the CSI Identity service which provides:
/// - Plugin identification (name and version)
/// - Plugin capability reporting
/// - Readiness probing
pub struct IdentityService {
    readiness: Option<Arc<ReadinessState>>,
}

impl IdentityService {
    /// Create a new IdentityService without shared readiness state
    /// (always reports ready for backward compatibility)
    pub fn new() -> Self {
        Self { readiness: None }
    }

    /// Create a new IdentityService with shared readiness state
    pub fn with_readiness(readiness: Arc<ReadinessState>) -> Self {
        Self {
            readiness: Some(readiness),
        }
    }
}

impl Default for IdentityService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl csi::identity_server::Identity for IdentityService {
    /// Returns the name and version of the CSI plugin.
    async fn get_plugin_info(
        &self,
        _request: Request<csi::GetPluginInfoRequest>,
    ) -> Result<Response<csi::GetPluginInfoResponse>, Status> {
        Ok(Response::new(csi::GetPluginInfoResponse {
            name: DRIVER_NAME.to_string(),
            vendor_version: DRIVER_VERSION.to_string(),
            manifest: std::collections::HashMap::new(),
        }))
    }

    /// Returns the capabilities of the CSI plugin.
    async fn get_plugin_capabilities(
        &self,
        _request: Request<csi::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<csi::GetPluginCapabilitiesResponse>, Status> {
        // Report capabilities: controller service and online volume expansion
        let capabilities = vec![
            csi::PluginCapability {
                r#type: Some(csi::plugin_capability::Type::Service(
                    csi::plugin_capability::Service {
                        r#type: csi::plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            },
            csi::PluginCapability {
                r#type: Some(csi::plugin_capability::Type::VolumeExpansion(
                    csi::plugin_capability::VolumeExpansion {
                        r#type: csi::plugin_capability::volume_expansion::Type::Online as i32,
                    },
                )),
            },
        ];

        Ok(Response::new(csi::GetPluginCapabilitiesResponse {
            capabilities,
        }))
    }

    /// Probes the plugin to check if it is ready.
    ///
    /// Returns ready=true when the driver has completed initialization
    /// and is accepting requests. Returns ready=false during startup
    /// or shutdown.
    async fn probe(
        &self,
        _request: Request<csi::ProbeRequest>,
    ) -> Result<Response<csi::ProbeResponse>, Status> {
        let ready = match &self.readiness {
            Some(state) => state.is_ready(),
            // Backward compatibility: if no readiness state provided, always ready
            None => true,
        };
        Ok(Response::new(csi::ProbeResponse { ready: Some(ready) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use csi::identity_server::Identity;

    #[tokio::test]
    async fn test_get_plugin_info() {
        let service = IdentityService::new();
        let request = Request::new(csi::GetPluginInfoRequest {});
        let response = Identity::get_plugin_info(&service, request).await.unwrap();
        let info = response.into_inner();

        assert_eq!(info.name, DRIVER_NAME);
        assert_eq!(info.vendor_version, DRIVER_VERSION);
    }

    #[tokio::test]
    async fn test_get_plugin_capabilities() {
        let service = IdentityService::new();
        let request = Request::new(csi::GetPluginCapabilitiesRequest {});
        let response = Identity::get_plugin_capabilities(&service, request)
            .await
            .unwrap();
        let caps = response.into_inner();

        // Should have 2 capabilities: controller service and volume expansion
        assert_eq!(caps.capabilities.len(), 2);
    }

    #[tokio::test]
    async fn test_probe() {
        let service = IdentityService::new();
        let request = Request::new(csi::ProbeRequest {});
        let response = Identity::probe(&service, request).await.unwrap();
        let probe = response.into_inner();

        assert_eq!(probe.ready, Some(true));
    }

    #[tokio::test]
    async fn test_probe_with_readiness_state() {
        use std::sync::Arc;

        let readiness = Arc::new(ReadinessState::new());
        let service = IdentityService::with_readiness(readiness.clone());

        // Initially not ready
        let request = Request::new(csi::ProbeRequest {});
        let response = Identity::probe(&service, request).await.unwrap();
        assert_eq!(response.into_inner().ready, Some(false));

        // Set ready
        readiness.set_ready(true);
        let request = Request::new(csi::ProbeRequest {});
        let response = Identity::probe(&service, request).await.unwrap();
        assert_eq!(response.into_inner().ready, Some(true));

        // Set not ready (shutdown)
        readiness.set_ready(false);
        let request = Request::new(csi::ProbeRequest {});
        let response = Identity::probe(&service, request).await.unwrap();
        assert_eq!(response.into_inner().ready, Some(false));
    }

    #[test]
    fn test_readiness_state() {
        let state = ReadinessState::new();
        assert!(!state.is_ready());

        state.set_ready(true);
        assert!(state.is_ready());

        state.set_ready(false);
        assert!(!state.is_ready());
    }
}
