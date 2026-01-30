use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::signal;
use tokio::sync::RwLock;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

use ctld_agent::ctl::CtlManager;
use ctld_agent::metrics;
use ctld_agent::service::StorageService;
use ctld_agent::service::proto::storage_agent_server::StorageAgentServer;
use ctld_agent::zfs::ZfsManager;

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

    /// Path to ctld config file (UCL format, used for portal/transport group validation)
    #[arg(long, env = "CTL_CONFIG_PATH", default_value = "/etc/ctl.conf")]
    ctl_config: PathBuf,

    /// Portal group name for iSCSI targets (used in UCL config)
    #[arg(long, env = "CTL_PORTAL_GROUP", default_value = "pg0")]
    portal_group: String,

    /// Transport group name for NVMeoF controllers (used in UCL config, FreeBSD 15.0+)
    #[arg(long, env = "CTL_TRANSPORT_GROUP", default_value = "tg0")]
    transport_group: String,

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

    /// Maximum concurrent storage operations (rate limiting)
    #[arg(long, env = "MAX_CONCURRENT_OPS", default_value = "10")]
    max_concurrent_ops: usize,

    /// Prometheus metrics HTTP address (e.g., 0.0.0.0:9091)
    /// If not set, metrics endpoint is disabled
    #[arg(long, env = "METRICS_ADDR")]
    metrics_addr: Option<String>,
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

    // Initialize Prometheus metrics endpoint if configured
    if let Some(ref addr_str) = args.metrics_addr {
        let addr = addr_str
            .parse()
            .map_err(|e| format!("Invalid metrics address '{}': {}", addr_str, e))?;
        if let Err(e) = metrics::init_metrics(addr) {
            return Err(format!("Failed to initialize metrics: {}", e).into());
        }
    }

    info!("Starting ctld-agent on {}", args.listen);
    info!("Log level: {}", args.log_level);
    info!("ZFS parent dataset: {}", args.zfs_parent);
    info!("Base IQN: {}", args.base_iqn);
    info!("Base NQN: {}", args.base_nqn);
    info!("CTL config path: {}", args.ctl_config.display());
    info!("Portal group: {}", args.portal_group);
    info!("Transport group name: {}", args.transport_group);
    info!("Max concurrent operations: {}", args.max_concurrent_ops);

    // Validate portal group exists if specified
    if !args.portal_group.is_empty() {
        ctld_agent::ctl::validate_portal_group_exists(&args.ctl_config, &args.portal_group)
            .await
            .map_err(|e| format!("Startup validation failed: {}", e))?;
        info!(
            "Validated portal-group '{}' exists in config",
            args.portal_group
        );
    }

    // Validate transport group exists if specified
    if !args.transport_group.is_empty() {
        ctld_agent::ctl::validate_transport_group_exists(&args.ctl_config, &args.transport_group)
            .await
            .map_err(|e| format!("Startup validation failed: {}", e))?;
        info!(
            "Validated transport-group '{}' exists in config",
            args.transport_group
        );
    }

    // Initialize ZFS manager
    let zfs_manager = ZfsManager::new(args.zfs_parent.clone()).await?;
    let zfs = Arc::new(RwLock::new(zfs_manager));

    // Initialize unified CTL manager for iSCSI and NVMeoF exports
    // Pass parent_dataset for device path validation (security: prevents privilege escalation)
    let ctl_manager = CtlManager::new(
        args.base_iqn.clone(),
        args.base_nqn.clone(),
        args.portal_group.clone(),
        args.transport_group.clone(),
        args.zfs_parent.clone(),
    )?;

    // Note: We intentionally do NOT load from UCL config here.
    // ZFS user properties are the source of truth for CSI-managed volumes.
    // Loading from UCL config would cause duplication if user-managed targets
    // happen to have our IQN/NQN prefix.

    let ctl = Arc::new(RwLock::new(ctl_manager));

    // Create the storage service with rate limiting
    let storage_service = StorageService::with_concurrency_limit(zfs, ctl, args.max_concurrent_ops);

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
            tracing::error!(
                "Failed to restore volume metadata from ZFS: {} - service starting in degraded mode",
                e
            );
            // Continue anyway - service can still operate on new volumes
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
            tracing::error!(
                "Failed to reconcile exports: {} - some volumes may need to be re-created",
                e
            );
            // Continue anyway - service can still operate on new volumes
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

    // Start the gRPC server with graceful shutdown
    builder
        .add_service(StorageAgentServer::new(storage_service))
        .serve_with_shutdown(addr, async {
            shutdown_signal().await;
            info!("Shutdown signal received, draining connections...");
        })
        .await?;

    info!("ctld-agent shutdown complete");
    Ok(())
}

/// Wait for shutdown signal (SIGTERM, SIGINT, or SIGHUP)
///
/// This function only supports Unix systems (FreeBSD/Linux) since the ctld-agent
/// exclusively runs on FreeBSD storage servers.
async fn shutdown_signal() {
    use signal::unix::{SignalKind, signal};

    // Install signal handlers, logging errors but continuing with available handlers
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!("Failed to install SIGTERM handler: {}", e);
            None
        }
    };

    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!("Failed to install SIGINT handler: {}", e);
            None
        }
    };

    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!("Failed to install SIGHUP handler: {}", e);
            None
        }
    };

    // Wait for any signal - use pending() for handlers that failed to install
    tokio::select! {
        _ = async { sigterm.as_mut().unwrap().recv().await }, if sigterm.is_some() => {
            info!("Received SIGTERM");
        }
        _ = async { sigint.as_mut().unwrap().recv().await }, if sigint.is_some() => {
            info!("Received SIGINT");
        }
        _ = async { sighup.as_mut().unwrap().recv().await }, if sighup.is_some() => {
            info!("Received SIGHUP (config reload not implemented, shutting down)");
        }
    }
}
