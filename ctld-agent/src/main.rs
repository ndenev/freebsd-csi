use std::sync::Arc;

use clap::Parser;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::info;

mod ctl;
mod service;
mod zfs;

use ctl::{IscsiManager, NvmeofManager, PortalGroup};
use service::proto::storage_agent_server::StorageAgentServer;
use service::StorageService;
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

    /// Portal group tag for iSCSI
    #[arg(long, default_value = "1")]
    portal_group: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("Starting ctld-agent on {}", args.listen);
    info!("ZFS parent dataset: {}", args.zfs_parent);
    info!("Base IQN: {}", args.base_iqn);
    info!("Base NQN: {}", args.base_nqn);
    info!("Portal group: {}", args.portal_group);

    // Initialize ZFS manager
    let zfs_manager = ZfsManager::new(args.zfs_parent.clone())?;
    let zfs = Arc::new(RwLock::new(zfs_manager));

    // Initialize iSCSI manager
    let portal_group = PortalGroup::new(args.portal_group, format!("pg{}", args.portal_group));
    let iscsi_manager = IscsiManager::new(args.base_iqn.clone(), portal_group)?;
    let iscsi = Arc::new(RwLock::new(iscsi_manager));

    // Initialize NVMeoF manager
    let nvmeof_manager = NvmeofManager::new(args.base_nqn.clone());
    let nvmeof = Arc::new(RwLock::new(nvmeof_manager));

    // Create the storage service
    let storage_service = StorageService::new(zfs, iscsi, nvmeof);

    // Parse the listen address
    let addr = args.listen.parse()?;

    info!("gRPC server listening on {}", addr);

    // Start the gRPC server
    Server::builder()
        .add_service(StorageAgentServer::new(storage_service))
        .serve(addr)
        .await?;

    Ok(())
}
