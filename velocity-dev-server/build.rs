fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the benchmark proto using protox (pure Rust — no protoc binary needed).
    // The proto lives in velocity-bench/proto/ so both the server
    // (velocity-dev-server) and client (velocity-bench) use the same contract.
    let fds = protox::compile(
        ["../velocity-bench/proto/benchmark.proto"],
        ["../velocity-bench/proto/"],
    )?;

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_fds(fds)?;
    Ok(())
}
