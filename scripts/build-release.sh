#!/usr/bin/env bash
set -euo pipefail

release_tag="${1:?release tag required}"
release_commit="${2:?Git commit required}"
release_arch="${3:?target architecture required}"
release_output="${4:-dist}"

if [[ ! "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo 'Release tags must have the form vMAJOR.MINOR.PATCH' >&2
  exit 1
fi
if [[ ! "$release_commit" =~ ^[0-9a-f]{7,64}$ ]]; then
  echo 'A hexadecimal Git commit is required' >&2
  exit 1
fi
case "$release_arch" in
  amd64|arm64) ;;
  *) echo 'Only amd64 and arm64 release builds are supported' >&2; exit 1 ;;
esac
release_version="${release_tag#v}"
release_stage="${release_output}/stage/linux-${release_arch}"
mkdir -p "$release_stage"
CGO_ENABLED=0 GOOS=linux GOARCH="$release_arch" go build -trimpath \
  -ldflags="-s -w -X main.version=${release_version} -X main.gitCommit=${release_commit}" \
  -o "$release_stage/qdrant-maintenance" ./cmd/qdrant-maintenance
tar -czf "${release_output}/qdrant-maintenance_${release_version}_linux_${release_arch}.tar.gz" \
  -C "$release_stage" qdrant-maintenance
