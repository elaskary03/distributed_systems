#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
PROXY_ADDR="127.0.0.1"
PROXY_PORT="9100"
PHOTO_ID="photo_test_001"
SECRET_FILE="/Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft/src/test.png"
COVER_FILE="/Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft/covers/default.png"
STEGO_OUT="photos/${PHOTO_ID}.png"
RECOVERED_FILE="recovered_${PHOTO_ID}.jpg"

# === Prepare ===
echo "=== Testing image send + stego pipeline ==="
echo "Secret file: ${SECRET_FILE}"
echo "Photo ID: ${PHOTO_ID}"
echo

# Show checksum of original secret
ORIG_CHECKSUM=$(shasum -a 256 "${SECRET_FILE}" | awk '{print $1}')
echo "Original secret checksum: ${ORIG_CHECKSUM}"

# === Send the photo to the proxy ===
SIZE=$(stat -f%z "${SECRET_FILE}")  # macOS stat usage
echo "Uploading secret (size=${SIZE} bytes)..."
printf "SEND_PHOTO %s %d\n" "${PHOTO_ID}" "${SIZE}" | nc ${PROXY_ADDR} ${PROXY_PORT}
cat "${SECRET_FILE}" | nc ${PROXY_ADDR} ${PROXY_PORT}
echo "Upload done."

# Wait a little for server to process
sleep 2

# === Check stego image exists ===
if [[ ! -f "${STEGO_OUT}" ]]; then
  echo "ERROR: Stego image ${STEGO_OUT} not found."
  exit 1
fi
echo "Stego image created: ${STEGO_OUT}"
ls -lh "${STEGO_OUT}"

# === Extract secret from stego (locally) ===
echo "Extracting secret from stego..."
# Assuming you have a small utility or Rust program that uses your extract code:
./extract_tool "${STEGO_OUT}" "${RECOVERED_FILE}"

# Show checksum of recovered secret
RECOV_CHECKSUM=$(shasum -a 256 "${RECOVERED_FILE}" | awk '{print $1}')
echo "Recovered secret checksum: ${RECOV_CHECKSUM}"

# === Verify ===
if [[ "${ORIG_CHECKSUM}" == "${RECOV_CHECKSUM}" ]]; then
  echo "✅ Success: recovered secret matches original."
else
  echo "❌ Failure: checksum mismatch."
  exit 2
fi

echo "=== Test completed successfully ==="
