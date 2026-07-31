//! Compiles murmur's `.proto` definitions into Rust (prost messages + tonic
//! service stubs) at build time. The generated code is pulled into the crate via
//! `tonic::include_proto!("murmur")` in `lib.rs`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/kv.proto", "proto/raft.proto"], &["proto"])?;
    Ok(())
}
