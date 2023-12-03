use std::io::Result;

fn main() -> Result<()> {
  prost_build::Config::new()
    .bytes(["."])
    .compile_protos(&["src/blob.proto"], &["src/"])?;
  Ok(())
}
