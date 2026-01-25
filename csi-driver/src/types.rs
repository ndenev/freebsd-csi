//! Type-safe wrappers for CSI parameters.
//!
//! These types provide compile-time safety for parameters that are parsed
//! from StorageClass parameters and volume contexts. Each type implements
//! `FromStr` for parsing at API boundaries and converts to proto types
//! when calling the ctld-agent.

use std::fmt::{self, Display};
use std::str::FromStr;

use crate::agent;

// ============================================================================
// ExportType
// ============================================================================

/// Storage export protocol type.
///
/// Determines whether volumes are exported via iSCSI or NVMeoF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportType {
    /// iSCSI protocol (default)
    #[default]
    Iscsi,
    /// NVMe over Fabrics protocol
    Nvmeof,
}

impl ExportType {
    /// Default port for this protocol.
    pub const fn default_port(self) -> u16 {
        match self {
            ExportType::Iscsi => 3260,
            ExportType::Nvmeof => 4420,
        }
    }
}

impl Display for ExportType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportType::Iscsi => write!(f, "iscsi"),
            ExportType::Nvmeof => write!(f, "nvmeof"),
        }
    }
}

impl FromStr for ExportType {
    type Err = ExportTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "iscsi" => Ok(ExportType::Iscsi),
            "nvmeof" | "nvme" => Ok(ExportType::Nvmeof),
            _ => Err(ExportTypeParseError(s.to_string())),
        }
    }
}

/// Error returned when parsing an invalid export type.
#[derive(Debug, Clone)]
pub struct ExportTypeParseError(String);

impl Display for ExportTypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown export type '{}': expected 'iscsi' or 'nvmeof'",
            self.0
        )
    }
}

impl std::error::Error for ExportTypeParseError {}

impl From<ExportType> for agent::ExportType {
    fn from(value: ExportType) -> Self {
        match value {
            ExportType::Iscsi => agent::ExportType::Iscsi,
            ExportType::Nvmeof => agent::ExportType::Nvmeof,
        }
    }
}

// ============================================================================
// CloneMode
// ============================================================================

/// Clone mode for creating volumes from snapshots.
///
/// Determines whether to use ZFS clone (linked, fast) or ZFS send/recv (copy, independent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CloneMode {
    /// Server chooses (defaults to Linked)
    #[default]
    Unspecified,
    /// Fast clone using ZFS clone (creates dependency on source)
    Linked,
    /// Full copy using ZFS send/recv (independent volume)
    Copy,
}

impl Display for CloneMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloneMode::Unspecified => write!(f, "unspecified"),
            CloneMode::Linked => write!(f, "linked"),
            CloneMode::Copy => write!(f, "copy"),
        }
    }
}

impl FromStr for CloneMode {
    type Err = CloneModeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "copy" | "independent" => Ok(CloneMode::Copy),
            "linked" | "clone" => Ok(CloneMode::Linked),
            "" | "unspecified" | "default" => Ok(CloneMode::Unspecified),
            _ => Err(CloneModeParseError(s.to_string())),
        }
    }
}

/// Error returned when parsing an invalid clone mode.
#[derive(Debug, Clone)]
pub struct CloneModeParseError(String);

impl Display for CloneModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown clone mode '{}': expected 'copy', 'linked', or 'unspecified'",
            self.0
        )
    }
}

impl std::error::Error for CloneModeParseError {}

impl From<CloneMode> for agent::CloneMode {
    fn from(value: CloneMode) -> Self {
        match value {
            CloneMode::Unspecified => agent::CloneMode::Unspecified,
            CloneMode::Linked => agent::CloneMode::Linked,
            CloneMode::Copy => agent::CloneMode::Copy,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

// ============================================================================
// ProvisioningMode
// ============================================================================

/// Volume provisioning mode for space allocation.
///
/// Controls whether ZFS reserves space upfront (thick) or allocates on write (thin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProvisioningMode {
    /// Thin provisioning (default): no reservation, space allocated on write
    #[default]
    Thin,
    /// Thick provisioning: refreservation=volsize, guarantees space upfront
    Thick,
}

impl ProvisioningMode {
    /// Parameter name in StorageClass parameters
    pub const PARAM_NAME: &'static str = "provisioningMode";

    /// Returns true if this mode requires space reservation
    pub const fn requires_reservation(self) -> bool {
        matches!(self, ProvisioningMode::Thick)
    }
}

impl Display for ProvisioningMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProvisioningMode::Thin => write!(f, "thin"),
            ProvisioningMode::Thick => write!(f, "thick"),
        }
    }
}

impl FromStr for ProvisioningMode {
    type Err = ProvisioningModeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "thin" | "" => Ok(ProvisioningMode::Thin),
            "thick" => Ok(ProvisioningMode::Thick),
            _ => Err(ProvisioningModeParseError(s.to_string())),
        }
    }
}

/// Error returned when parsing an invalid provisioning mode.
#[derive(Debug, Clone)]
pub struct ProvisioningModeParseError(String);

impl Display for ProvisioningModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown provisioning mode '{}': expected 'thin' or 'thick'",
            self.0
        )
    }
}

impl std::error::Error for ProvisioningModeParseError {}

// ============================================================================
// Endpoint
// ============================================================================

/// A storage target endpoint (host:port).
///
/// Represents a single endpoint for iSCSI or NVMeoF connections.
/// The host can be an IP address (v4 or v6) or a hostname - no resolution is attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// Host address (IP or hostname, not resolved)
    pub host: String,
    /// Port number
    pub port: u16,
}

impl Endpoint {
    /// Create a new endpoint with explicit host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Format as "host:port" string for platform functions.
    pub fn to_portal_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Error returned when parsing an invalid endpoint.
#[derive(Debug, Clone)]
pub struct EndpointParseError(String);

impl Display for EndpointParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid endpoint '{}': expected 'host:port' or 'host'", self.0)
    }
}

impl std::error::Error for EndpointParseError {}

/// A list of endpoints for multipath support.
///
/// Parses comma-separated endpoints from volume_context and provides
/// the full list to platform functions for multipath connections.
#[derive(Debug, Clone)]
pub struct Endpoints {
    endpoints: Vec<Endpoint>,
}

impl Endpoints {
    /// Parse endpoints from a comma-separated string with a default port.
    ///
    /// Format: "host1:port1,host2:port2,..." or "host1,host2,..." (uses default_port)
    /// Supports IPv4, IPv6 (with brackets for port), and hostnames.
    ///
    /// # Examples
    /// - "10.0.0.1:3260,10.0.0.2:3260" → two endpoints with explicit ports
    /// - "10.0.0.1,10.0.0.2" → two endpoints with default port
    /// - "[::1]:3260" → IPv6 with port
    /// - "storage.local:3260" → hostname with port
    pub fn parse(s: &str, default_port: u16) -> Result<Self, EndpointParseError> {
        let mut endpoints = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let endpoint = Self::parse_single(part, default_port)?;
            endpoints.push(endpoint);
        }

        if endpoints.is_empty() {
            return Err(EndpointParseError(s.to_string()));
        }

        Ok(Self { endpoints })
    }

    /// Parse a single endpoint string.
    fn parse_single(s: &str, default_port: u16) -> Result<Endpoint, EndpointParseError> {
        // Handle IPv6 with brackets: [::1]:port
        if s.starts_with('[') {
            if let Some(bracket_end) = s.find(']') {
                let host = &s[1..bracket_end];
                let rest = &s[bracket_end + 1..];

                if rest.is_empty() {
                    return Ok(Endpoint::new(host, default_port));
                } else if let Some(port_str) = rest.strip_prefix(':') {
                    let port = port_str
                        .parse::<u16>()
                        .map_err(|_| EndpointParseError(s.to_string()))?;
                    return Ok(Endpoint::new(host, port));
                }
            }
            return Err(EndpointParseError(s.to_string()));
        }

        // Handle host:port or host (use rfind for IPv6 without brackets)
        // For "host:port", the last colon separates host from port
        // For IPv6 without brackets like "::1", we can't distinguish, so treat as host-only
        if let Some(colon_idx) = s.rfind(':') {
            let potential_host = &s[..colon_idx];
            let potential_port = &s[colon_idx + 1..];

            // If potential_port is numeric, treat as host:port
            // Otherwise treat the whole string as a host (e.g., IPv6 "::1")
            if let Ok(port) = potential_port.parse::<u16>()
                && !potential_host.is_empty()
            {
                return Ok(Endpoint::new(potential_host, port));
            }
        }

        // No valid port found, use default
        Ok(Endpoint::new(s, default_port))
    }

    /// Get the list of endpoints.
    pub fn as_slice(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// Get the number of endpoints.
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Check if multipath (more than one endpoint).
    pub fn is_multipath(&self) -> bool {
        self.endpoints.len() > 1
    }

    /// Format all endpoints as comma-separated "host:port" strings.
    /// This is the format expected by platform::connect_iscsi/connect_nvmeof.
    pub fn to_portal_string(&self) -> String {
        self.endpoints
            .iter()
            .map(|e| e.to_portal_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Get the first endpoint (for single-path fallback or display).
    pub fn first(&self) -> Option<&Endpoint> {
        self.endpoints.first()
    }
}

impl IntoIterator for Endpoints {
    type Item = Endpoint;
    type IntoIter = std::vec::IntoIter<Endpoint>;

    fn into_iter(self) -> Self::IntoIter {
        self.endpoints.into_iter()
    }
}

impl<'a> IntoIterator for &'a Endpoints {
    type Item = &'a Endpoint;
    type IntoIter = std::slice::Iter<'a, Endpoint>;

    fn into_iter(self) -> Self::IntoIter {
        self.endpoints.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_type_from_str() {
        assert_eq!("iscsi".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("ISCSI".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("iScSi".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("nvmeof".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("NVMEOF".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("nvme".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("NvMe".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert!("unknown".parse::<ExportType>().is_err());
    }

    #[test]
    fn test_export_type_display() {
        assert_eq!(ExportType::Iscsi.to_string(), "iscsi");
        assert_eq!(ExportType::Nvmeof.to_string(), "nvmeof");
    }

    #[test]
    fn test_export_type_default_port() {
        assert_eq!(ExportType::Iscsi.default_port(), 3260);
        assert_eq!(ExportType::Nvmeof.default_port(), 4420);
    }

    #[test]
    fn test_clone_mode_from_str() {
        assert_eq!("copy".parse::<CloneMode>().unwrap(), CloneMode::Copy);
        assert_eq!("independent".parse::<CloneMode>().unwrap(), CloneMode::Copy);
        assert_eq!("linked".parse::<CloneMode>().unwrap(), CloneMode::Linked);
        assert_eq!("clone".parse::<CloneMode>().unwrap(), CloneMode::Linked);
        assert_eq!("".parse::<CloneMode>().unwrap(), CloneMode::Unspecified);
        assert_eq!(
            "unspecified".parse::<CloneMode>().unwrap(),
            CloneMode::Unspecified
        );
        assert!("unknown".parse::<CloneMode>().is_err());
    }

    #[test]
    fn test_clone_mode_display() {
        assert_eq!(CloneMode::Unspecified.to_string(), "unspecified");
        assert_eq!(CloneMode::Linked.to_string(), "linked");
        assert_eq!(CloneMode::Copy.to_string(), "copy");
    }

    #[test]
    fn test_export_type_to_proto() {
        let proto: agent::ExportType = ExportType::Iscsi.into();
        assert_eq!(proto, agent::ExportType::Iscsi);

        let proto: agent::ExportType = ExportType::Nvmeof.into();
        assert_eq!(proto, agent::ExportType::Nvmeof);
    }

    #[test]
    fn test_clone_mode_to_proto() {
        let proto: agent::CloneMode = CloneMode::Unspecified.into();
        assert_eq!(proto, agent::CloneMode::Unspecified);

        let proto: agent::CloneMode = CloneMode::Linked.into();
        assert_eq!(proto, agent::CloneMode::Linked);

        let proto: agent::CloneMode = CloneMode::Copy.into();
        assert_eq!(proto, agent::CloneMode::Copy);
    }

    #[test]
    fn test_provisioning_mode_from_str() {
        assert_eq!(
            "thin".parse::<ProvisioningMode>().unwrap(),
            ProvisioningMode::Thin
        );
        assert_eq!(
            "THIN".parse::<ProvisioningMode>().unwrap(),
            ProvisioningMode::Thin
        );
        assert_eq!(
            "".parse::<ProvisioningMode>().unwrap(),
            ProvisioningMode::Thin
        );
        assert_eq!(
            "thick".parse::<ProvisioningMode>().unwrap(),
            ProvisioningMode::Thick
        );
        assert_eq!(
            "THICK".parse::<ProvisioningMode>().unwrap(),
            ProvisioningMode::Thick
        );
        assert!("unknown".parse::<ProvisioningMode>().is_err());
    }

    #[test]
    fn test_provisioning_mode_display() {
        assert_eq!(ProvisioningMode::Thin.to_string(), "thin");
        assert_eq!(ProvisioningMode::Thick.to_string(), "thick");
    }

    #[test]
    fn test_provisioning_mode_requires_reservation() {
        assert!(!ProvisioningMode::Thin.requires_reservation());
        assert!(ProvisioningMode::Thick.requires_reservation());
    }

    // Endpoint tests

    #[test]
    fn test_endpoint_new_and_display() {
        let ep = Endpoint::new("10.0.0.1", 3260);
        assert_eq!(ep.host, "10.0.0.1");
        assert_eq!(ep.port, 3260);
        assert_eq!(ep.to_string(), "10.0.0.1:3260");
        assert_eq!(ep.to_portal_string(), "10.0.0.1:3260");
    }

    #[test]
    fn test_endpoints_parse_single() {
        let eps = Endpoints::parse("10.0.0.1:3260", 9999).unwrap();
        assert_eq!(eps.len(), 1);
        assert!(!eps.is_multipath());
        assert_eq!(eps.first().unwrap().host, "10.0.0.1");
        assert_eq!(eps.first().unwrap().port, 3260);
    }

    #[test]
    fn test_endpoints_parse_single_default_port() {
        let eps = Endpoints::parse("10.0.0.1", 3260).unwrap();
        assert_eq!(eps.len(), 1);
        assert_eq!(eps.first().unwrap().host, "10.0.0.1");
        assert_eq!(eps.first().unwrap().port, 3260);
    }

    #[test]
    fn test_endpoints_parse_multipath() {
        let eps = Endpoints::parse("10.0.0.1:3260,10.0.0.2:3260", 9999).unwrap();
        assert_eq!(eps.len(), 2);
        assert!(eps.is_multipath());
        assert_eq!(eps.to_portal_string(), "10.0.0.1:3260,10.0.0.2:3260");
    }

    #[test]
    fn test_endpoints_parse_multipath_default_ports() {
        let eps = Endpoints::parse("10.0.0.1,10.0.0.2", 4420).unwrap();
        assert_eq!(eps.len(), 2);
        assert!(eps.is_multipath());
        assert_eq!(eps.to_portal_string(), "10.0.0.1:4420,10.0.0.2:4420");
    }

    #[test]
    fn test_endpoints_parse_mixed_ports() {
        let eps = Endpoints::parse("10.0.0.1:3260,10.0.0.2", 4420).unwrap();
        assert_eq!(eps.len(), 2);
        let endpoints: Vec<_> = eps.as_slice().to_vec();
        assert_eq!(endpoints[0].port, 3260);
        assert_eq!(endpoints[1].port, 4420);
    }

    #[test]
    fn test_endpoints_parse_with_whitespace() {
        let eps = Endpoints::parse("  10.0.0.1:3260 , 10.0.0.2:3260  ", 9999).unwrap();
        assert_eq!(eps.len(), 2);
        assert_eq!(eps.to_portal_string(), "10.0.0.1:3260,10.0.0.2:3260");
    }

    #[test]
    fn test_endpoints_parse_hostname() {
        let eps = Endpoints::parse("storage.example.com:3260", 9999).unwrap();
        assert_eq!(eps.first().unwrap().host, "storage.example.com");
        assert_eq!(eps.first().unwrap().port, 3260);
    }

    #[test]
    fn test_endpoints_parse_hostname_default_port() {
        let eps = Endpoints::parse("storage.example.com", 3260).unwrap();
        assert_eq!(eps.first().unwrap().host, "storage.example.com");
        assert_eq!(eps.first().unwrap().port, 3260);
    }

    #[test]
    fn test_endpoints_parse_ipv6_bracketed() {
        let eps = Endpoints::parse("[::1]:3260", 9999).unwrap();
        assert_eq!(eps.first().unwrap().host, "::1");
        assert_eq!(eps.first().unwrap().port, 3260);
    }

    #[test]
    fn test_endpoints_parse_ipv6_bracketed_default_port() {
        let eps = Endpoints::parse("[2001:db8::1]", 3260).unwrap();
        assert_eq!(eps.first().unwrap().host, "2001:db8::1");
        assert_eq!(eps.first().unwrap().port, 3260);
    }

    #[test]
    fn test_endpoints_parse_empty_fails() {
        assert!(Endpoints::parse("", 3260).is_err());
        assert!(Endpoints::parse("   ", 3260).is_err());
    }

    #[test]
    fn test_endpoints_iterate() {
        let eps = Endpoints::parse("10.0.0.1:3260,10.0.0.2:3260", 9999).unwrap();
        let hosts: Vec<_> = eps.into_iter().map(|e| e.host).collect();
        assert_eq!(hosts, vec!["10.0.0.1", "10.0.0.2"]);
    }
}
