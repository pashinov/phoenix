fn main() {
    protoc_rust::Codegen::new()
        .out_dir("src")
        .input("proto/phoenix.proto")
        .run()
        .unwrap();
}
