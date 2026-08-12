fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the benchmark proto — same contract used by velocity-bench client.
    // This server implements BenchmarkService on top of the production WorkflowEngine.
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
