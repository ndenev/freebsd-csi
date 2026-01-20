//! FreeBSD CSI Driver
//!
//! Kubernetes CSI driver that implements the Container Storage Interface
//! and communicates with the ctld-agent for iSCSI target management.

use std::path::PathBuf;

use clap::Parser;
use tracing::{debug, info, Level};
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

pub use agent_client::{AgentClient, TlsConfig};
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
    #[arg(long, env = "AGENT_ENDPOINT", default_value = "http://127.0.0.1:50051")]
    agent_endpoint: String,

    /// Run in controller mode (enables controller service)
    #[arg(long, default_value = "false")]
    controller: bool,

    /// Run in node mode (enables node service)
    #[arg(long, default_value = "true")]
    node: bool,

    /// Driver name
    #[arg(long, default_value = "csi.freebsd.org")]
    driver_name: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// TLS certificate file (PEM format)
    #[arg(long, env = "TLS_CERT_PATH")]
    tls_cert: Option<PathBuf>,

    /// TLS private key file (PEM format)
    #[arg(long, env = "TLS_KEY_PATH")]
    tls_key: Option<PathBuf>,

    /// CA certificate for server verification
    #[arg(long, env = "TLS_CA_PATH")]
    tls_ca: Option<PathBuf>,

    /// TLS domain name (for server certificate verification)
    #[arg(long, env = "TLS_DOMAIN", default_value = "ctld-agent")]
    tls_domain: String,
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

    // Parse CSI endpoint
    let endpoint = args.endpoint.clone();

    // Create services and build gRPC server
    use tonic::transport::Server;
    use csi::controller_server::ControllerServer;
    use csi::identity_server::IdentityServer;
    use csi::node_server::NodeServer;

    let identity = IdentityService::new();
    let mut server = Server::builder();
    let mut router = server.add_service(IdentityServer::new(identity));

    if args.controller {
        info!("Enabling Controller service");

        // Build TLS config if all required paths are provided
        let tls_config = match (&args.tls_cert, &args.tls_key, &args.tls_ca) {
            (Some(cert), Some(key), Some(ca)) => {
                debug!(
                    cert = %cert.display(),
                    key = %key.display(),
                    ca = %ca.display(),
                    "TLS certificate paths configured"
                );
                info!(domain = %args.tls_domain, "mTLS enabled for agent connection");
                Some(TlsConfig {
                    cert_path: cert.clone(),
                    key_path: key.clone(),
                    ca_path: ca.clone(),
                    domain: args.tls_domain.clone(),
                })
            }
            (None, None, None) => {
                info!("TLS disabled - using plaintext connection to agent");
                None
            }
            _ => {
                return Err("TLS configuration incomplete: all of --tls-cert, --tls-key, and --tls-ca must be provided together".into());
            }
        };

        let controller = ControllerService::with_tls(args.agent_endpoint.clone(), tls_config);
        router = router.add_service(ControllerServer::new(controller));
    }

    if args.node {
        info!("Enabling Node service");
        let node_svc = NodeService::new(node_id.clone());
        router = router.add_service(NodeServer::new(node_svc));
    }

    // Start server based on endpoint type
    if endpoint.starts_with("unix://") {
        let path = endpoint.strip_prefix("unix://").unwrap();

        // Create parent directory if needed
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Remove existing socket file
        let _ = std::fs::remove_file(path);

        // Use tokio UnixListener for Unix sockets
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::UnixListenerStream;

        let listener = UnixListener::bind(path)?;
        let stream = UnixListenerStream::new(listener);

        info!("CSI driver listening on {}", endpoint);
        router.serve_with_incoming(stream).await?;
    } else {
        // TCP endpoint
        let addr = endpoint.parse()?;
        info!("CSI driver listening on {}", addr);
        router.serve(addr).await?;
    }

    Ok(())
}
