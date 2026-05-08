#!/usr/bin/env bash
# Example llama.cpp server script — second backend instance
#
# This script starts a second llama.cpp server for multi-backend
# load balancing. Each backend should use a different port and
# ideally a different GPU group.
#
# KVBridge expects the server to listen on the port specified in config/config.json.
# This example uses port 11435.

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────
# These should differ from the first backend's settings.

MODEL="${MODEL_PATH:-./models/qwen2.5-7b-instruct-q4_k_m.gguf}"
HOST="${LLAMA_HOST:-127.0.0.1}"
PORT="${LLAMA_PORT:-11435}"
THREADS="${LLAMA_THREADS:-8}"
GPU_LAYERS="${LLAMA_GPU_LAYERS:-35}"

# ─── Start llama.cpp server ──────────────────────────────────
exec llama-server \
  --model "$MODEL" \
  --host "$HOST" \
  --port "$PORT" \
  --threads "$THREADS" \
  --tensor-split 1.0 \
  --n-gpu-layers "$GPU_LAYERS" \
  --ctx-size 8192 \
  --batch-size 2048 \
  "$@"
