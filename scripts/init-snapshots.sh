#!/bin/sh

set -eu

ALIAS="${RUSTFS_ALIAS:-rustfs}"
BUCKET="${RUSTFS_BUCKET:-qdrant-snapshots}"

COLLECTION="documents"
NODE="${QDRANT_NODE_ID:-qdrant-0}"
SNAPSHOT_COUNT=6

echo "Creating ${SNAPSHOT_COUNT} test snapshots..."

i=1
while [ "$i" -le "$SNAPSHOT_COUNT" ]; do
    filename="snapshot-${i}.snapshot"
    key="${COLLECTION}/${NODE}/${filename}"
    tmpfile="/tmp/${filename}"

    echo "test snapshot ${i}" > "${tmpfile}"

    echo "Uploading ${key}"
    rc cp "${tmpfile}" "${ALIAS}/${BUCKET}/${key}"

    rm "${tmpfile}"

    # Ensure LastModified differs between snapshots.
    sleep 1

    i=$((i + 1))
done

echo "Test snapshots created."