fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile CSI proto (from official CSI spec)
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../proto/csi.proto"], &["../proto"])?;

    // Compile agent proto for client
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["../proto/ctld_agent.proto"], &["../proto"])?;

    Ok(())
}
