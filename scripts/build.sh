#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_DIR="${FRONTEND_DIR:-$(dirname "$ROOT")/FileCabinetFront}"
(cd "$FRONTEND_DIR" && bun install --frozen-lockfile && bun run build)
# Only public is generated. Frontend source remains in its own repository.
find "$ROOT/public" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
cp -a "$FRONTEND_DIR/dist/." "$ROOT/public/"
mkdir -p "$ROOT/bin"
(cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -o bin/filecabinet .)
printf '\nBuilt %s/bin/filecabinet with embedded UI\n' "$ROOT"
