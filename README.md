# Mnemosyne

> KV Cache Semantic Router — an inverse API gateway for llama.cpp that caches and restores KV state by hashing system prompts.

Named after the Greek titaness of memory, Mnemosyne speeds up repeated prompts with the same system message by saving and restoring the KV cache state on disk, avoiding redundant processing of large system prompts.

## Architecture

```
OpenClaw ──→ Mnemosyne (:8080) ──→ llama.cpp (:11434)
                   │
                   ▼
            /home/daniel/Models/slot_cache/
              *.bin files (KV cache state)
```

Mnemosyne sits between your application (OpenClaw) and llama.cpp as a middleware proxy. It intercepts chat completion requests, hashes the system prompt, and either serves from disk cache or saves state after the first full pass.

## How it works

1. **Hash** — The system prompt is SHA-256 hashed to produce a unique cache key.
2. **Cache HIT** — The corresponding `.bin` file exists → Mnemosyne restores the KV cache for the slot, then proxies the request with user messages only.
3. **Cache MISS** — No file found → Mnemosyne proxies the full request (system prompt + messages), then saves the KV cache to disk in the background after the response stream completes.
4. **No system prompt** — Requests without a system prompt bypass caching entirely and are forwarded directly to llama.cpp.

## Getting started

### Prerequisites

- Node.js ≥ 18 (for native `fetch`)
- llama.cpp running on `http://127.0.0.1:11434`

### Installation

```bash
npm install fastify @fastify/cors @fastify/formbody
```

### Running

```bash
node server.js
```

Mnemosyne listens on `:8080` and proxies to llama.cpp on `:11434`.

### Configuration

Edit the constants at the top of `server.js`:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | Port Mnemosyne listens on |
| `LLAMA_HOST` | `127.0.0.1` | Host where llama.cpp is running |
| `LLAMA_PORT` | `11434` | Port where llama.cpp is running |
| `CACHE_DIR` | `/home/daniel/Models/slot_cache` | Directory for `.bin` cache files |

## API

Mnemosyne exposes the same OpenAI-compatible endpoint as llama.cpp:

```
POST /v1/chat/completions
```

Send standard OpenAI-format chat completion requests. The proxy handles everything transparently — cache lookup, KV state management, and SSE streaming.

## Response headers

Mnemosyne adds a `X-Cache-Status` header to SSE responses:

| Value | Meaning |
|---|---|
| `HIT` | System prompt was found in cache; KV state was restored from disk |
| `MISS` | First request for this system prompt; KV state was saved after streaming |

## Logging

All operations are logged to the terminal with color-coded tags:

- **■ CACHE HIT** (green) — KV state restored from disk
- **■ CACHE MISS** (yellow) — Full request proxied, cache save queued
- **SAVE** — Background KV cache save operation
- **RESTORE** — KV cache restore operation
- **✖ ERROR** (red) — Errors and failures

## Project structure

```
server.js          # Single-file Fastify server with all logic
slot_cache/        # KV cache .bin files (auto-created on first run)
package.json       # Dependencies
```

## License
