#!/usr/bin/env bash
# bootstrap.sh — end-to-end example: install, init, register 4 upstreams, print API key + URLs.
#
# After it runs:
#   1) The router is installed to ~/.local/bin/polyglotmesh
#   2) A sample config is at $POLYGLOTMESH_HOME/config.sample.toml (for reference)
#   3) The active config is at $POLYGLOTMESH_HOME/config.toml — EDIT THIS to add upstreams,
#      change limits, add keys, etc.
#   4) `polyglotmesh show` prints the merged config; `polyglotmesh where` prints the path.
set -euo pipefail

# ---- CONFIGURE ME ----
OPENAI_URLS=(
  "https://api.openai.com/v1"
  # "https://api.openrouter.ai/v1"
  # "http://gpu-box.local:8000/v1"
)
OPENAI_KEYS=(
  "sk-..."
  # "sk-or-..."
  # "EMPTY"
)
OPENAI_MODELS="gpt-4o-mini,gpt-4o"

ANTHROPIC_URLS=(
  "https://api.anthropic.com"
  # "https://bedrock-runtime.us-east-1.amazonaws.com"
)
ANTHROPIC_KEYS=(
  "sk-ant-..."
  # "bedrock-key"
)
ANTHROPIC_MODELS="claude-3-5-sonnet-20241022,claude-3-5-haiku-20241022"

BIND="0.0.0.0:8080"
# ----------------------

cd "$(dirname "$0")/.."

if [[ ! -x "$HOME/.local/bin/polyglotmesh" ]]; then
  ./scripts/install.sh --prefix "$HOME/.local/bin"
fi

AILR="$HOME/.local/bin/polyglotmesh"

export POLYGLOTMESH_HOME="${POLYGLOTMESH_HOME:-$HOME/.polyglotmesh}"
mkdir -p "$POLYGLOTMESH_HOME"

# Copy the sample config in (only if there's no live config yet).
if [[ ! -f "$POLYGLOTMESH_HOME/config.toml" ]]; then
  if [[ -f examples/config.sample.toml ]]; then
    cp examples/config.sample.toml "$POLYGLOTMESH_HOME/config.sample.toml"
    echo "==> sample config copied to $POLYGLOTMESH_HOME/config.sample.toml"
  fi
fi

# Reset live config for a clean run.
rm -f "$POLYGLOTMESH_HOME/config.toml"

# `init` prints the generated API key.
echo "==> initializing config in $POLYGLOTMESH_HOME"
"$AILR" init --bind "$BIND"

# OpenAI upstreams
for i in "${!OPENAI_URLS[@]}"; do
  id="openai-$((i+1))"
  url="${OPENAI_URLS[$i]}"
  key="${OPENAI_KEYS[$i]:-}"
  [[ -z "$key" ]] && { echo "skipping $id (no api key)"; continue; }
  echo "==> adding upstream $id -> $url"
  "$AILR" upstream add \
    --id "$id" \
    --kind openai \
    --base-url "$url" \
    --api-key "$key" \
    --models "$OPENAI_MODELS" \
    --priority "$((30 - i*10))"
done

# Anthropic upstreams
for i in "${!ANTHROPIC_URLS[@]}"; do
  id="anthropic-$((i+1))"
  url="${ANTHROPIC_URLS[$i]}"
  key="${ANTHROPIC_KEYS[$i]:-}"
  [[ -z "$key" ]] && { echo "skipping $id (no api key)"; continue; }
  echo "==> adding upstream $id -> $url"
  "$AILR" upstream add \
    --id "$id" \
    --kind anthropic \
    --base-url "$url" \
    --api-key "$key" \
    --models "$ANTHROPIC_MODELS" \
    --priority "$((30 - i*10))"
done

cat <<EOF

==================================================
polyglotmesh is configured.
Edit the active config to add per-key limits, aliases, etc:
    $AILR where
    \$EDITOR $POLYGLOTMESH_HOME/config.toml
    $AILR show     # print the merged config

Sample config (for reference, with every field documented):
    $POLYGLOTMESH_HOME/config.sample.toml

Start:                 $AILR serve
OpenAI base:           http://$BIND/v1
Anthropic base:        http://$BIND/v1   (POST /v1/messages)
==================================================
EOF
