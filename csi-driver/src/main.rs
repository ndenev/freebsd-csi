//! FreeBSD CSI Driver
//!
//! Kubernetes CSI driver that implements the Container Storage Interface
//! and communicates with the ctld-agent for iSCSI target management.

use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// CSI proto generated types
pub mod csi {
    tonic::include_proto!("csi.v1");
}

/// ctld-agent proto generated types (client)
pub mod agent {
    tonic::include_proto!("ctld_agent.v1");
}

mod agent_client;
mod controller;
mod identity;
mod node;

pub use agent_client::AgentClient;
pub use controller::ControllerService;
pub use identity::{IdentityService, DRIVER_NAME, DRIVER_VERSION};
pub use node::NodeService;

/// CLI arguments for the CSI driver
#[derive(Parser, Debug)]
#[command(name = "csi-driver")]
#[command(about = "FreeBSD CSI Driver for Kubernetes")]
struct Args {
    /// CSI endpoint (unix socket path)
    #[arg(long, default_value = "unix:///var/run/csi/csi.sock")]
    endpoint: String,

    /// Node ID for this CSI node
    #[arg(long, env = "CSI_NODE_ID")]
    node_id: Option<String>,

    /// ctld-agent gRPC endpoint
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    agent_endpoint: String,

    /// Run in controller mode (enables controller service)
    #[arg(long, default_value = "false")]
    controller: bool,

    /// Run in node mode (enables node service)
    #[arg(long, default_value = "true")]
    node: bool,

    /// Driver name
    #[arg(long, default_value = "freebsd.csi.io")]
    driver_name: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize tracing
    let level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Determine node_id
    let node_id = match args.node_id {
        Some(id) => id,
        None => hostname::get()?.to_string_lossy().to_string(),
    };

    info!(
        driver_name = %args.driver_name,
        endpoint = %args.endpoint,
        agent_endpoint = %args.agent_endpoint,
        node_id = %node_id,
        controller_mode = %args.controller,
        node_mode = %args.node,
        "Starting FreeBSD CSI Driver"
    );

    // TODO: Implement CSI services
    // - Identity Service (required)
    // - Controller Service (if controller mode)
    // - Node Service (if node mode)

    info!("CSI Driver placeholder - services not yet implemented");

    Ok(())
}
