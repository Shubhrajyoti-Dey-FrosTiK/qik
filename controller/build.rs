fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("proto/structure.proto")?;
    tonic_build::configure()
        .compile_well_known_types(true)
        // .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".google.protobuf", "::prost_types")
        .compile_protos(&["proto/controller.proto"], &["proto"])
        .unwrap();
    Ok(())
}
