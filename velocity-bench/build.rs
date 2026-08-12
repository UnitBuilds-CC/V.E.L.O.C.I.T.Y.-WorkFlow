fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the benchmark proto for both client (velocity-bench) and
    // server (velocity-dev-server) use.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(&["proto/benchmark.proto"], &["proto/"])?;
    Ok(())
}
