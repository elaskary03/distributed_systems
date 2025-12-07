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
/// [8-byte big-endian u64 total_len | nonce | ciphertext]
pub fn pack_embed_blob(nonce: &Nonce, ciphertext: &[u8]) -> Vec<u8> {
    let nonce_bytes = nonce.as_slice();
    let total_len = nonce_bytes.len() + ciphertext.len();
    let mut out = Vec::with_capacity(8 + total_len);
    out.extend_from_slice(&(total_len as u64).to_be_bytes());
    out.extend_from_slice(nonce_bytes);
    out.extend_from_slice(ciphertext);
    out
}

/// Unpack our stego payload format back into (nonce, ciphertext).
pub fn unpack_embed_blob(data: &[u8]) -> Result<(Nonce, Vec<u8>)> {
    if data.len() < 8 + 12 {
        bail!("embed blob too small");
    }
    let total_len = u64::from_be_bytes(data[..8].try_into().unwrap()) as usize;
    if total_len < 12 {
        bail!("embed blob length too small");
    }
    if data.len() < 8 + total_len {
        bail!("embed blob truncated (expected {} bytes)", total_len);
    }

    let payload = &data[8..8 + total_len];
    let mut nonce_arr = [0u8; 12];
    nonce_arr.copy_from_slice(&payload[..12]);
    let nonce = *Nonce::from_slice(&nonce_arr);
    let ciphertext = payload[12..].to_vec();

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

/// Extract the full `[len | nonce | ciphertext]` blob from the image using a length prefix.
pub fn extract_payload(img: &RgbaImage) -> Result<(Nonce, Vec<u8>)> {
    // Read length prefix (u64 BE)
    let len_prefix = extract_n_bytes(img, 8)?;
    if len_prefix.len() != 8 {
        bail!("header truncated (got {} bytes)", len_prefix.len());
    }
    let total_len = u64::from_be_bytes(len_prefix[..8].try_into().unwrap()) as usize;
    if total_len < 12 {
        bail!("embedded payload too small for nonce");
    }

    let need_total = 8 + total_len;
    let have_bits = img.width() as usize * img.height() as usize * 4;
    let have_total = have_bits / 8;
    if need_total > have_total {
        bail!(
            "not enough bits in image to extract payload: need {} bytes (~{} bits), have ~{} bytes",
            need_total,
            need_total * 8,
            have_total
        );
    }

    let all = extract_n_bytes(img, need_total)?;
    let (nonce, ciphertext) = unpack_embed_blob(&all)?;
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
        .write_to(
            &mut std::io::Cursor::new(&mut stego_bytes),
            ImageOutputFormat::Png,
        )
        .context("encode stego PNG")?;

    // Compute stats
    let ct_sha = sha256_hex(&ciphertext);
    let bytes_embedded = payload.len();

    Ok((stego_bytes, ct_sha, bytes_embedded))
}

/// Extracts the embedded payload from a stego PNG and decrypts it with the passphrase.
/// Returns the original plaintext bytes.
pub fn extract_and_decrypt_from_png(passphrase: &[u8], stego_png_bytes: &[u8]) -> Result<Vec<u8>> {
    // 1) Decode PNG -> RGBA
    let rgba = image::load_from_memory(stego_png_bytes)
        .context("decode stego PNG")?
        .to_rgba8();

    // 2) Pull out [nonce | len | ciphertext] from LSBs
    let (nonce, ciphertext) = extract_payload(&rgba)?;

    // 3) Decrypt
    let plaintext =
        decrypt_bytes(passphrase, &nonce, &ciphertext).context("decrypt embedded ciphertext")?;

    Ok(plaintext)
}
