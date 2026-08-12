fn main() {
    // Proto compilation is optional — the CLI uses REST API by default.
    // To enable gRPC, install protoc and set PROTOC env variable.
    if std::env::var("PROTOC").is_ok() || which_protoc().is_some() {
        let proto_files = &[
            "../proto/velocity/v1/workflow_service.proto",
            "../proto/velocity/v1/common.proto",
            "../proto/velocity/v1/messages.proto",
        ];

        let includes = &["../proto"];

        if proto_files.iter().all(|f| std::path::Path::new(f).exists()) {
            let _ = tonic_build::configure()
                .build_server(false)
                .build_client(true)
                .compile_protos(proto_files, includes);
        }
    }
}

fn which_protoc() -> Option<String> {
    std::env::var("PROTOC").ok().or_else(|| {
        // Check common locations
        let paths = ["protoc", "/usr/bin/protoc", "/usr/local/bin/protoc"];
        paths.iter().find_map(|p| {
            std::process::Command::new(p)
                .arg("--version")
                .output()
                .ok()
                .map(|_| p.to_string())
        })
    })
}
