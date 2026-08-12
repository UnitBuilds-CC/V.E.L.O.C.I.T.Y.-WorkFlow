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
        proto_root.join("velocity/v1/common.proto"),
        proto_root.join("velocity/v1/messages.proto"),
        proto_root.join("velocity/v1/errordetails.proto"),
        proto_root.join("velocity/v1/workflow_service.proto"),
    ];

    let includes = &[proto_root];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(protos, includes)?;

    // Re-run if any proto file changes
    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    Ok(())
}

#[cfg(not(feature = "grpc"))]
fn main() {
    // No-op when grpc feature is not enabled
}
