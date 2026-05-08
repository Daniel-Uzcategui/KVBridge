#!/usr/bin/env bash
# Example llama.cpp server script for KVBridge
#
# This script starts a llama.cpp server instance.
# Customize the model path, GPU layers, and other options for your setup.
#
# KVBridge expects the server to listen on the port specified in config/config.json.
# This example uses port 11434.

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────
# Adjust these to match your GPU setup and model.

MODEL="${MODEL_PATH:-./models/qwen2.5-7b-instruct-q4_k_m.gguf}"
HOST="${LLAMA_HOST:-127.0.0.1}"
PORT="${LLAMA_PORT:-11434}"
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
