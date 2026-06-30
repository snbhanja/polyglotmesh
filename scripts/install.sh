#!/usr/bin/env bash
# install.sh — Build and install the polyglotmesh binary.
#
# Usage:
#   ./scripts/install.sh                       # install to ~/.local/bin
#   ./scripts/install.sh --prefix /opt/pgm    # custom prefix
#   ./scripts/install.sh --skip-build          # just copy an already-built binary
#
# After install, run:
#   polyglotmesh init
#   polyglotmesh serve
#   # then edit the config file (printed by `polyglotmesh where`) to add upstreams and limits.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PREFIX="${HOME}/.local/bin"
SKIP_BUILD=0
PROFILE="release"
TARGET_DIR="$PROJECT_DIR/target"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --debug) PROFILE="debug"; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

cd "$PROJECT_DIR"

if [[ $SKIP_BUILD -eq 0 ]]; then
  if ! command -v cargo >/dev/null 2>&1; then
    echo "==> cargo not found; installing rustup (stable, minimal profile)..." >&2
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
  fi
  echo "==> building polyglotmesh ($PROFILE)..." >&2
  cargo build --"$PROFILE"
fi

BIN_SRC="$TARGET_DIR/$PROFILE/polyglotmesh"
if [[ ! -x "$BIN_SRC" ]]; then
  echo "binary not found at $BIN_SRC" >&2
  exit 1
fi

mkdir -p "$PREFIX"
cp -f "$BIN_SRC" "$PREFIX/polyglotmesh"
chmod +x "$PREFIX/polyglotmesh"

echo "==> installed to $PREFIX/polyglotmesh" >&2

# Drop the sample config alongside the live one so users can diff / copy.
HOME_DIR="${POLYGLOTMESH_HOME:-$HOME/.polyglotmesh}"
mkdir -p "$HOME_DIR"
SAMPLE_SRC="$PROJECT_DIR/examples/config.sample.toml"
SAMPLE_DST="$HOME_DIR/config.sample.toml"
if [[ -f "$SAMPLE_SRC" ]]; then
  cp -f "$SAMPLE_SRC" "$SAMPLE_DST"
  echo "==> sample config written to: $SAMPLE_DST" >&2
fi

echo "==> next steps:" >&2
echo "       polyglotmesh init --bind 0.0.0.0:8080        # generate API key + admin token" >&2
echo "       \$EDITOR $HOME_DIR/config.toml                  # add upstreams + per-key limits" >&2
echo "       polyglotmesh where                            # print the active config path" >&2
echo "       polyglotmesh show                             # print the merged config" >&2
echo "       polyglotmesh serve                            # start the router" >&2
