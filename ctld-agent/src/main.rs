use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::RwLock;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

mod ctl;
mod service;
mod zfs;

use ctl::CtlManager;
use service::StorageService;
use service::proto::storage_agent_server::StorageAgentServer;
use zfs::ZfsManager;

#[derive(Parser, Debug)]
#[command(name = "ctld-agent")]
#[command(about = "FreeBSD ZFS/CTL storage agent for Kubernetes CSI")]
struct Args {
    /// gRPC listen address
    #[arg(long, default_value = "[::1]:50051")]
    listen: String,

    /// ZFS parent dataset for volumes
    #[arg(long)]
    zfs_parent: String,

    /// Base iSCSI IQN (e.g., iqn.2024-01.com.example.storage)
    #[arg(long, default_value = "iqn.2024-01.org.freebsd.csi")]
    base_iqn: String,

    /// Base NVMeoF NQN (e.g., nqn.2024-01.com.example.storage)
    #[arg(long, default_value = "nqn.2024-01.org.freebsd.csi")]
    base_nqn: String,

    /// Path to ctld UCL config file
    #[arg(long, env = "CTL_CONFIG_PATH", default_value = "/etc/ctl.ucl")]
    ctl_config: PathBuf,

    /// Auth group name for iSCSI targets
    #[arg(long, env = "CTL_AUTH_GROUP", default_value = "ag0")]
    auth_group: String,

    /// Portal group name for iSCSI targets (used in UCL config)
    #[arg(long, env = "CTL_PORTAL_GROUP_NAME", default_value = "pg0")]
    portal_group_name: String,

    /// Transport group name for NVMeoF controllers (used in UCL config, FreeBSD 15.0+)
    #[arg(long, env = "CTL_TRANSPORT_GROUP_NAME", default_value = "tg0")]
    transport_group_name: String,

    /// TLS certificate file (PEM format)
    #[arg(long, env = "TLS_CERT_PATH")]
    tls_cert: Option<PathBuf>,

    /// TLS private key file (PEM format)
    #[arg(long, env = "TLS_KEY_PATH")]
    tls_key: Option<PathBuf>,

    /// CA certificate for client verification (enables mTLS)
    #[arg(long, env = "TLS_CLIENT_CA_PATH")]
    tls_client_ca: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize tracing with configured log level
    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting ctld-agent on {}", args.listen);
    info!("Log level: {}", args.log_level);
    info!("ZFS parent dataset: {}", args.zfs_parent);
    info!("Base IQN: {}", args.base_iqn);
    info!("Base NQN: {}", args.base_nqn);
    info!("CTL config path: {}", args.ctl_config.display());
    info!("Auth group: {}", args.auth_group);
    info!("Portal group name: {}", args.portal_group_name);
    info!("Transport group name: {}", args.transport_group_name);

    // Initialize ZFS manager
    let zfs_manager = ZfsManager::new(args.zfs_parent.clone())?;
    let zfs = Arc::new(RwLock::new(zfs_manager));

    // Initialize unified CTL manager for iSCSI and NVMeoF exports
    let mut ctl_manager = CtlManager::new(
        args.base_iqn.clone(),
        args.base_nqn.clone(),
        args.portal_group_name.clone(),
        args.ctl_config.to_string_lossy().to_string(),
        args.auth_group.clone(),
        args.transport_group_name.clone(),
    )?;

    // Load existing exports from UCL config (startup recovery)
    if let Err(e) = ctl_manager.load_config() {
        tracing::warn!("Failed to load existing exports from UCL config: {}", e);
        // Continue anyway - service can still operate
    }

    let ctl = Arc::new(RwLock::new(ctl_manager));

    // Create the storage service
    let storage_service = StorageService::new(zfs, ctl);

    // Restore volume metadata from ZFS user properties
    match storage_service.restore_from_zfs().await {
        Ok(count) => {
            if count > 0 {
                info!(
                    "Successfully restored {} volume(s) from ZFS metadata",
                    count
                );
            }
        }
        Err(e) => {
            tracing::warn!("Failed to restore volume metadata from ZFS: {}", e);
            // Continue anyway - service can still operate
        }
    }

    // Reconcile exports: ensure all volumes in ZFS metadata are exported
    match storage_service.reconcile_exports().await {
        Ok(count) => {
            if count > 0 {
                info!(
                    "Reconciled {} export(s) that were missing from CTL config",
                    count
                );
            }
        }
        Err(e) => {
            tracing::warn!("Failed to reconcile exports: {}", e);
            // Continue anyway - service can still operate
        }
    }

    // Parse the listen address
    let addr = args.listen.parse()?;

    info!("gRPC server listening on {}", addr);

    // Build the gRPC server with optional TLS
    let mut builder = Server::builder();

    // Configure TLS if certificates provided
    if let (Some(cert_path), Some(key_path)) = (&args.tls_cert, &args.tls_key) {
        let cert = tokio::fs::read(cert_path).await?;
        let key = tokio::fs::read(key_path).await?;
        let identity = Identity::from_pem(cert, key);

        let mut tls = ServerTlsConfig::new().identity(identity);

        // If client CA provided, require client certificates (mTLS)
        if let Some(ca_path) = &args.tls_client_ca {
            let ca = tokio::fs::read(ca_path).await?;
            tls = tls.client_ca_root(Certificate::from_pem(ca));
            info!("mTLS enabled - client certificates required");
        } else {
            info!("TLS enabled (server-only, no client verification)");
        }

        builder = builder.tls_config(tls)?;
    } else {
        info!("TLS disabled - running in plaintext mode");
    }

    // Start the gRPC server
    builder
        .add_service(StorageAgentServer::new(storage_service))
        .serve(addr)
        .await?;

    Ok(())
}
