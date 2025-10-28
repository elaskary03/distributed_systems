// Bring in your module
mod stego;
use std::fs;

fn main() -> anyhow::Result<()> {
    // 1️⃣ Read your two images into memory
    let cover = fs::read("cover.png")?;      // the normal image (big enough)
    let secret = fs::read("secret.png")?;    // the image you want to hide

    // 2️⃣ Hide the secret image inside the cover
    let stego = stego::stego_wrap(&cover, &secret)?;
    fs::write("stego.png", &stego)?;          // save the new image

    // 3️⃣ Extract the hidden image back out
    let recovered = stego::stego_unwrap(&stego)?;
    fs::write("recovered.png", &recovered)?;  // save it again

    println!("✅  Done!  Open stego.png and recovered.png to check.");
    Ok(())
}
