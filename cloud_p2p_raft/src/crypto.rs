//! Shared crypto + stego helpers for GUI and Node.
//!
//! Encoding format embedded into image LSBs:
//! [ nonce:12 | cipher_len:4 (BE u32) | ciphertext:cipher_len bytes ]
//!
//! We use ChaCha20-Poly1305 (AEAD) and a simple 1-bit-per-channel (RGBA)
//! least-significant-bit (LSB) embedding scheme.

use anyhow::{bail, Context, Result};
use chacha20poly1305::{aead::Aead, aead::KeyInit, ChaCha20Poly1305, Key, Nonce};
use image::{DynamicImage, RgbaImage};
use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha256};

/// Derive a 32-byte key from an arbitrary passphrase via SHA-256.
/// If you want stronger KDF properties, wrap this with Argon2 or PBKDF2 upstream.
pub fn derive_key(passphrase: &[u8]) -> Key {
    let mut h = Sha256::new();
    h.update(passphrase);
    let out = h.finalize(); // 32 bytes
    *Key::from_slice(&out)
}

/// Generate a random 12-byte nonce suitable for ChaCha20-Poly1305.
pub fn rand_nonce() -> Nonce {
    let mut n = [0u8; 12];
    OsRng.fill_bytes(&mut n);
    *Nonce::from_slice(&n)
}

/// AEAD encrypt arbitrary bytes with passphrase-derived key.
/// Returns (nonce, ciphertext).
pub fn encrypt_bytes(passphrase: &[u8], plaintext: &[u8]) -> Result<(Nonce, Vec<u8>)> {
    let key = derive_key(passphrase);
    let nonce = rand_nonce();
    let cipher = ChaCha20Poly1305::new(&key);
    let ct = cipher
        .encrypt(&nonce, plaintext)
        .context("chacha20poly1305 encrypt")?;
    Ok((nonce, ct))
}

/// AEAD decrypt given (nonce, ciphertext) with passphrase-derived key.
pub fn decrypt_bytes(passphrase: &[u8], nonce: &Nonce, ciphertext: &[u8]) -> Result<Vec<u8>> {
    let key = derive_key(passphrase);
    let cipher = ChaCha20Poly1305::new(&key);
    let pt = cipher
        .decrypt(nonce, ciphertext)
        .context("chacha20poly1305 decrypt")?;
    Ok(pt)
}

/// Pack (nonce, ciphertext) into our stego payload format:
/// [12 bytes nonce][4 bytes big-endian length][ciphertext bytes]
pub fn pack_embed_blob(nonce: &Nonce, ciphertext: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + 4 + ciphertext.len());
    out.extend_from_slice(nonce.as_ref());
    out.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
    out.extend_from_slice(ciphertext);
    out
}

/// Unpack our stego payload format back into (nonce, ciphertext).
pub fn unpack_embed_blob(data: &[u8]) -> Result<(Nonce, Vec<u8>)> {
    if data.len() < 12 + 4 {
        bail!("embed blob too small");
    }
    let nonce_bytes = &data[..12];
    let len_bytes = &data[12..16];
    let cipher_len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;

    if data.len() < 16 + cipher_len {
        bail!("embed blob truncated (expected {} bytes of ciphertext)", cipher_len);
    }

    let mut nonce_arr = [0u8; 12];
    nonce_arr.copy_from_slice(nonce_bytes);
    let nonce = *Nonce::from_slice(&nonce_arr);
    let ciphertext = data[16..16 + cipher_len].to_vec();

    Ok((nonce, ciphertext))
}

/// Return capacity *in bytes* for 1 LSB per RGBA channel:
/// capacity_bytes = (width * height * 4) / 8
pub fn lsb_capacity_bytes(img: &RgbaImage) -> usize {
    (img.width() as usize * img.height() as usize * 4) / 8
}

/// Embed `data` into the LSBs of the provided RGBA image (clone),
/// writing 1 bit per channel in raster order. Returns a PNG-ready image.
pub fn embed_lsb_rgba(img: &RgbaImage, data: &[u8]) -> Result<DynamicImage> {
    let need_bits = data.len() * 8;
    let have_bits = img.width() as usize * img.height() as usize * 4;
    if need_bits > have_bits {
        bail!(
            "not enough capacity: need {} bits ({} bytes) but have {} bits (~{} bytes)",
            need_bits,
            data.len(),
            have_bits,
            have_bits / 8
        );
    }

    let mut out = img.clone();
    let mut bit_idx = 0usize;

    for y in 0..out.height() {
        for x in 0..out.width() {
            let mut px = *out.get_pixel(x, y);
            for ch in 0..4 {
                if bit_idx >= need_bits {
                    out.put_pixel(x, y, px);
                    return Ok(DynamicImage::ImageRgba8(out));
                }
                let byte = data[bit_idx / 8];
                let bit = (byte >> (7 - (bit_idx % 8))) & 1;
                px[ch] = (px[ch] & 0xFE) | bit;
                bit_idx += 1;
            }
            out.put_pixel(x, y, px);
        }
    }

    // If we exit the loops without returning, we embedded everything.
    Ok(DynamicImage::ImageRgba8(out))
}

/// Extract `n_bytes` from the image's LSBs (1 bit per channel, RGBA, raster order).
pub fn extract_n_bytes(img: &RgbaImage, n_bytes: usize) -> Result<Vec<u8>> {
    let need_bits = n_bytes * 8;
    let have_bits = img.width() as usize * img.height() as usize * 4;
    if need_bits > have_bits {
        bail!(
            "not enough bits in image to extract {} bytes (have ~{} bytes)",
            n_bytes,
            have_bits / 8
        );
    }

    let mut out = vec![0u8; n_bytes];
    let mut bit_idx = 0usize;

    for y in 0..img.height() {
        for x in 0..img.width() {
            let px = img.get_pixel(x, y);
            for ch in 0..4 {
                if bit_idx >= need_bits {
                    return Ok(out);
                }
                let bit = px[ch] & 1;
                let byte_idx = bit_idx / 8;
                let bit_pos = 7 - (bit_idx % 8);
                out[byte_idx] |= bit << bit_pos;
                bit_idx += 1;
            }
        }
    }

    Ok(out)
}

/// Extract the full `[nonce | len | ciphertext]` blob from the image:
/// 1) first read 16 bytes (nonce + length),
/// 2) parse cipher length, then read exactly that many more bytes,
/// 3) return (nonce, ciphertext).
pub fn extract_payload(img: &RgbaImage) -> Result<(Nonce, Vec<u8>)> {
    // step 1: read header (12 + 4 = 16 bytes)
    let header = extract_n_bytes(img, 16)?;
    let (nonce, _dummy) = unpack_embed_blob(&header)?;
    // step 2: read the length to know how many more bytes we need
    let cipher_len = u32::from_be_bytes(header[12..16].try_into().unwrap()) as usize;

    // total we need is 16 + cipher_len
    let total = 16 + cipher_len;
    let all = extract_n_bytes(img, total)?;
    let (nonce2, ciphertext) = unpack_embed_blob(&all)?;
    // sanity: both nonces should match
    if nonce != nonce2 {
        bail!("nonce mismatch while extracting payload");
    }
    Ok((nonce, ciphertext))
}

/// Convenience: compute SHA-256 hex of bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Encrypts plaintext bytes, embeds them into the given RGBA image, and
/// returns a tuple: (stego PNG bytes, ciphertext SHA-256 hex, bytes_embedded).
pub fn encrypt_and_embed_to_png(
    passphrase: &[u8],
    plaintext: &[u8],
    img_bytes: &[u8],
) -> Result<(Vec<u8>, String, usize)> {
    use image::ImageOutputFormat;

    // Decode image
    let img = image::load_from_memory(img_bytes)
        .context("decode input image")?
        .to_rgba8();

    // Encrypt data
    let (nonce, ciphertext) = encrypt_bytes(passphrase, plaintext)?;
    let payload = pack_embed_blob(&nonce, &ciphertext);

    // Embed
    let stego_img = embed_lsb_rgba(&img, &payload)?;

    // Encode stego image as PNG bytes
    let mut stego_bytes = Vec::new();
    stego_img
        .write_to(&mut std::io::Cursor::new(&mut stego_bytes), ImageOutputFormat::Png)
        .context("encode stego PNG")?;

    // Compute stats
    let ct_sha = sha256_hex(&ciphertext);
    let bytes_embedded = payload.len();

    Ok((stego_bytes, ct_sha, bytes_embedded))
}
