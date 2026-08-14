fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the benchmark proto using protox (pure Rust — no protoc binary needed).
    // The proto file lives at velocity-bench/proto/benchmark.proto relative to workspace root.
    let proto_path = "../../velocity-bench/proto/benchmark.proto";
    let proto_dir = "../../velocity-bench/proto";

    // When building inside Docker the proto may be at a different path
    let (resolved_path, resolved_dir) = if std::path::Path::new(proto_path).exists() {
        (proto_path, proto_dir)
    } else if std::path::Path::new("/bench-proto/benchmark.proto").exists() {
        ("/bench-proto/benchmark.proto", "/bench-proto")
    } else {
        eprintln!("WARNING: benchmark.proto not found, gRPC client will not be available");
        return Ok(());
    };

    let fds = protox::compile([resolved_path], [resolved_dir])?;

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_fds(fds)?;

    Ok(())
}
