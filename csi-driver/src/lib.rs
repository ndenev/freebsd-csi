//! FreeBSD CSI Driver Library
//!
//! Kubernetes CSI driver that implements the Container Storage Interface
//! and communicates with the ctld-agent for iSCSI/NVMeoF target management.
//!
//! This library provides:
//! - CSI Identity, Controller, and Node service implementations
//! - Agent client for communication with ctld-agent
//! - Platform-specific mount/unmount operations

/// CSI proto generated types
pub mod csi {
    tonic::include_proto!("csi.v1");
}

/// ctld-agent proto generated types (client)
pub mod agent {
    tonic::include_proto!("ctld_agent.v1");
}

pub mod agent_client;
pub mod controller;
pub mod identity;
pub mod metrics;
pub mod node;
pub mod platform;

pub use agent_client::AgentClient;
pub use controller::ControllerService;
pub use identity::IdentityService;
pub use node::NodeService;
