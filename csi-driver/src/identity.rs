//! CSI Identity Service Implementation
//!
//! Provides plugin identification and capability reporting to Kubernetes.

use tonic::{Request, Response, Status};

use crate::csi;

pub const DRIVER_NAME: &str = "freebsd.csi.io";
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// CSI Identity Service
///
/// Implements the CSI Identity service which provides:
/// - Plugin identification (name and version)
/// - Plugin capability reporting
/// - Readiness probing
pub struct IdentityService;

impl IdentityService {
    pub fn new() -> Self {
        Self
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

        Ok(Response::new(csi::GetPluginCapabilitiesResponse { capabilities }))
    }

    /// Probes the plugin to check if it is ready.
    async fn probe(
        &self,
        _request: Request<csi::ProbeRequest>,
    ) -> Result<Response<csi::ProbeResponse>, Status> {
        // The plugin is ready when this service is running
        Ok(Response::new(csi::ProbeResponse { ready: Some(true) }))
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
        let response = Identity::get_plugin_capabilities(&service, request).await.unwrap();
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
}
