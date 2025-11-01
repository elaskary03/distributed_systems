use clap::Parser;
use std::{fs, path::PathBuf};
use cloud_p2p_raft::crypto::extract_and_decrypt_from_png;

#[derive(Parser, Debug)]
#[command(author, version, about="Client-side stego decrypt/extract")]
struct Args {
    /// Stego PNG to read (produced by the cluster)
    #[arg(long)]
    stego: PathBuf,

    /// Passphrase used at encryption time
    #[arg(long)]
    passphrase: String,

    /// Output file to write the original recovered bytes
    #[arg(long)]
    out: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let stego_bytes = fs::read(&args.stego)?;
    let plaintext = extract_and_decrypt_from_png(args.passphrase.as_bytes(), &stego_bytes)?;
    if let Some(parent) = args.out.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }
    fs::write(&args.out, &plaintext)?;
    println!("OK wrote {}", args.out.display());
    Ok(())
}
