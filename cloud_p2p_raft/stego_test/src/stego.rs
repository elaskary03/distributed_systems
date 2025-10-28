// src/stego.rs
use anyhow::{Result, ensure};
use image::{io::Reader as ImgReader, ImageFormat, RgbaImage};

const HEADER_LEN: usize = 8; // store payload length as u64 (LE)

/* Hide `secret` bytes into the LSB of a PNG cover.
   Returns PNG bytes of the stego image. */
pub fn stego_wrap(cover_png: &[u8], secret: &[u8]) -> Result<Vec<u8>> {
    // 1) load cover as RGBA
    let img = ImgReader::new(std::io::Cursor::new(cover_png))
        .with_guessed_format()?.decode()?;
    let mut rgba: RgbaImage = img.to_rgba8();

    // 2) capacity check: using 1 LSB per byte in the raw buffer
    // capacity_bits == number of bytes in pixel buffer
    let cap_bits = rgba.as_raw().len();
    let need_bits = (HEADER_LEN + secret.len()) * 8;
    ensure!(need_bits <= cap_bits, "payload too big for this cover image");

    // 3) payload = [len:8 bytes LE] + secret
    let mut payload = Vec::with_capacity(HEADER_LEN + secret.len());
    payload.extend_from_slice(&(secret.len() as u64).to_le_bytes());
    payload.extend_from_slice(secret);

    // 4) write 1 bit of payload into LSB of each byte in raw buffer
    let raw = rgba.as_mut();
    let mut bit_i = 0;
    for &byte in &payload {
        for b in 0..8 {
            let bit = (byte >> b) & 1;
            let v = raw[bit_i];
            raw[bit_i] = (v & 0xFE) | bit;
            bit_i += 1;
        }
    }

    // 5) re-encode PNG
    let mut out = Vec::new();
    image::DynamicImage::ImageRgba8(rgba)
        .write_to(&mut std::io::Cursor::new(&mut out), ImageFormat::Png)?;
    Ok(out)
}

/* Extract hidden bytes from stego PNG. */
pub fn stego_unwrap(stego_png: &[u8]) -> Result<Vec<u8>> {
    let img = ImgReader::new(std::io::Cursor::new(stego_png))
        .with_guessed_format()?.decode()?;
    let rgba = img.to_rgba8();
    let raw = rgba.as_raw();

    // 1) read first 64 bits (8 bytes) as length (little-endian)
    let mut len_bytes = [0u8; 8];
    for i in 0..64 {
        let bit = raw[i] & 1;
        len_bytes[i / 8] |= bit << (i % 8);
    }
    let len = u64::from_le_bytes(len_bytes) as usize;

    // 2) read `len` bytes after header
    let start = 64;
    let total_bits = start + len * 8;
    ensure!(total_bits <= raw.len(), "stego image too small/corrupted");

    let mut secret = vec![0u8; len];
    let mut idx = 0;
    for byte_i in 0..len {
        let mut byte = 0u8;
        for b in 0..8 {
            let bit = raw[start + idx] & 1;
            byte |= bit << b;
            idx += 1;
        }
        secret[byte_i] = byte;
    }

    Ok(secret)
}
