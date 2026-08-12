//! Build script for compiling protobuf definitions with tonic-build.
//!
//! This script is only active when the `grpc` feature is enabled.
//! It compiles the proto files in `proto/velocity/v1/` into Rust types
//! and gRPC service stubs.

#[cfg(feature = "grpc")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::path::PathBuf;

    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("velocity-workflow-engine should be inside VELOCITY-WorkFlow")
        .join("proto");

    let protos = &[
        "velocity/v1/common.proto",
        "velocity/v1/messages.proto",
        "velocity/v1/errordetails.proto",
        "velocity/v1/workflow_service.proto",
        "velocity/v1/health_service.proto",
        "velocity/v1/history_service.proto",
        "velocity/v1/matching_service.proto",
        "velocity/v1/worker_service.proto",
        "velocity/v1/admin_service.proto",
    ];

    // Use protox (pure Rust) instead of prost-build (requires protoc binary)
    let fds = protox::compile(protos, [&proto_root])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;

    // Re-run if any proto file changes
    for proto in protos {
        println!(
            "cargo:rerun-if-changed={}",
            proto_root.join(proto).display()
        );
    }

    Ok(())
}

#[cfg(not(feature = "grpc"))]
fn main() {
    // No-op when grpc feature is not enabled
}
