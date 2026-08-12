fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the benchmark proto using protox (pure Rust — no protoc binary needed).
    // Both velocity-bench (client) and velocity-dev-server (server) use
    // the same benchmark.proto — ensuring identical message schemas.
    //
    // We generate both client AND server code because:
    // - velocity-bench uses the client (GrpcAdapter)
    // - temporal-bridge uses the server (BenchmarkService impl)
    let fds = protox::compile(["proto/benchmark.proto"], ["proto/"])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    Ok(())
}
