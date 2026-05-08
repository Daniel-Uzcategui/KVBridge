# Mnemosyne

> **KV Cache Semantic Router — Tiered Storage (L1 RAM / L2 SSD)**

Named after the Greek titaness of memory, Mnemosyne speeds up repeated prompts with the same system message by saving and restoring the KV cache state across a two-tier storage system — a fast volatile RAMDisk (L1) backed by persistent SSD storage (L2).

## Architecture

```
OpenClaw ──→ Mnemosyne (:8080) ──→ llama.cpp (:11434)
                   │                      │
        ┌──────────┴──────────┐     GPU 0,1 ── <gpu>
        ▼                     ▼     GPU 2,3 ── <gpu>
  L1: RAMDisk (tmpfs)    L2: SSD (persistent)
  $HOME/Models/          $HOME/Models/
  ramdisk_cache/         slot_cache/
    *.bin files            *.bin files
```

Mnemosyne sits between your application (OpenClaw) and llama.cpp as a middleware proxy. It intercepts chat completion requests, hashes the system prompt (and optionally large RAG payloads), and either serves from the fast L1 RAMDisk cache or falls back through L2 SSD storage.

A web dashboard at `http://localhost:8080/dashboard` provides real-time visibility into GPU telemetry, backend health, cache hits, and active slots.

## Features

### Tiered Storage (L1 + L2)

- **L1 (RAMDisk / tmpfs)** — Volatile, ultra-fast storage with configurable capacity. Hot cache entries live here for instant KV cache restoration.
- **L2 (SSD / persistent)** — Unlimited, persistent storage. Cache entries survive restarts and are copied to L1 on demand (cold hits).

### Cache Hit Types

| Type | Description |
|---|---|
| **Warm HIT** | `.bin` file exists in L1 → restored immediately from RAMDisk |
| **Cold HIT** | `.bin` file only in L2 → copied L2→L1, then restored from RAMDisk |
| **MISS** | No cache entry → full request proxied, KV cache saved to L1 then persisted to L2 |

### Dynamic Slot Load Balancing

A promise-based FIFO concurrency queue limits the number of simultaneous requests forwarded to llama.cpp. When `--parallel N` is configured on llama.cpp, set `MAX_CONCURRENT_SLOTS` to match. Excess requests queue automatically — none are dropped.

### L1 Eviction Sweeper

When total L1 size exceeds `L1_EVICT_THRESHOLD` (default 2.5 GB), the oldest `.bin` files are evicted until usage drops to `L1_EVICT_TARGET` (default 2.0 GB). This prevents the RAMDisk from filling up.

### LRU Garbage Collector

A background sweeper runs on a configurable interval, scanning L1 for `.bin` files older than `MAX_FILE_AGE`. Expired files are deleted to prevent disk overflow. The GC logs each deletion with file size and age.

### Advanced RAG-aware Prefix Hashing

When a first user message exceeds `RAG_CONTENT_LENGTH_THRESHOLD` characters, it is concatenated with the system prompt before hashing. This ensures the cache key represents the entire static context, maximizing hits for repeated RAG injections regardless of whether the context lives in the system prompt or the first user message.

### Startup Pre-Warm

On startup, the N most recently modified `.bin` files from L2 are copied to L1, warming the cache before the first request arrives.

### Ghost Detector Logger

All incoming requests (with system prompts and first user messages) are logged to `ghost_detector.txt` for debugging and analysis.

### GPU Telemetry

Mnemosyne collects live GPU metrics using `nvidia-smi` and exposes them via the `/api/stats` endpoint. The dashboard UI renders each GPU with:

- Temperature (°C) with color-coded badges (green < 65, yellow 65-75, red > 75)
- Fan speed (%)
- GPU utilization (%)
- Power draw (W)
- Memory usage (used / total MiB) with progress bar
- P-State

A summary card shows average temperature, total power across all GPUs, and peak utilization.

### Multi-Backend Support

Mnemosyne can manage multiple llama.cpp backends, each assigned to specific GPU groups. Backends are configured in `settings.js` and `config/config.json`. The dashboard shows backend health, process status (starting/running/stopped/error), and allows manual start/stop/restart.

### Dashboard

A web-based dashboard at `/dashboard` provides real-time visibility into:

- **GPU Telemetry** — Live `nvidia-smi` metrics for all installed GPUs
- **Backend Processes** — Health, status, and process details per backend
- **Active Hardware Slots** — Slot allocation across all backends
- **Top Cache Entries** — Ranked cache entries with hit counts
- **Backend Logs** — Selectable logs per backend for debugging

## How it works

1. **Hash** — The system prompt (and optionally the first user message if it exceeds the RAG threshold) is SHA-256 hashed to produce a unique cache key.
2. **Slot Acquisition** — Every request acquires a concurrency slot from the FIFO queue before contacting llama.cpp.
3. **Cache HIT (Warm)** — The corresponding `.bin` file exists in L1 → Mnemosyne restores the KV cache for the slot, then proxies the request with user messages only.
4. **Cold HIT** — The `.bin` file exists in L2 only → copied to L1, then KV cache restored and request proxied.
5. **Cache MISS** — No file found → Mnemosyne proxies the full request (system prompt + messages), then saves the KV cache to L1 and persists to L2 in the background after the response stream completes.
6. **No system prompt** — Requests without a system prompt bypass caching entirely and are forwarded directly to llama.cpp.
7. **Eviction & GC** — Periodically, L1 is checked for over-threshold usage (evicting oldest files) and expired files are purged.

## Getting started

### Prerequisites

- Node.js ≥ 18 (for native `fetch`)
- [llama.cpp](https://github.com/ggerganov/llama.cpp) built and available on `PATH`
- A tmpfs/RAMDisk mounted at `$HOME/Models/ramdisk_cache` (L1) — or any fast storage location

### Backend Scripts

Copy the example scripts and customize them for your GPU setup:

```bash
cp config/start_server.example.sh ~/Models/start_server.sh
cp config/start_server2.example.sh ~/Models/start_server2.sh
# Edit the MODEL, GPU_LAYERS, and PORT variables in each script
```

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
| `L1_DIR` | `.../ramdisk_cache` | L1 RAMDisk directory (volatile, fast) |
| `L2_DIR` | `.../slot_cache` | L2 SSD directory (persistent) |
| `MAX_CONCURRENT_SLOTS` | `3` | Max simultaneous requests to llama.cpp |
| `L1_EVICT_THRESHOLD` | `2.5 GB` | Evict L1 files when total size exceeds this |
| `L1_EVICT_TARGET` | `2.0 GB` | Evict down to this threshold |
| `GARBAGE_COLLECT_INTERVAL_MS` | `300000` (5 min) | GC sweeper interval |
| `MAX_FILE_AGE_MS` | `43200000` (12 h) | Max age before GC deletion |
| `RAG_CONTENT_LENGTH_THRESHOLD` | `1000` | First user message length threshold for prefix hashing |
| `WARMUP_TOP_N` | `15` | Number of L2 files to pre-warm into L1 on startup |

## API

Mnemosyne exposes the same OpenAI-compatible endpoint as llama.cpp:

```
POST /v1/chat/completions
```

Send standard OpenAI-format chat completion requests. The proxy handles everything transparently — cache lookup, KV state management, and SSE streaming.

### GET /api/stats

Returns a JSON snapshot of system state:

```json
{
  "queueLength": 0,
  "l1UsageBytes": 3221225472,
  "gpu": {
    "devices": [
      {
        "index": 0,
        "name": "NVIDIA <gpu>",
        "temperatureC": 85,
        "fanSpeedPercent": 59,
        "utilizationGpu": 100,
        "powerDrawWatts": 200.71,
        "memoryUsedMiB": 10233,
        "memoryTotalMiB": 11264,
        "pstate": "P2"
      }
    ],
    "summary": {
      "averageTempC": 80,
      "totalPowerWatts": 425,
      "busiestUtilization": 100
    }
  },
  "slots": [...],
  "llamaMetrics": {},
  "backends": [...]
}
```

The `gpu` field is populated by running `nvidia-smi --query-gpu=index,name,temperature.gpu,fan.speed,utilization.gpu,power.draw,memory.used,memory.total,pstate --format=csv,noheader,nounits`.

### GET /api/settings / POST /api/settings

Get and update runtime settings (persisted to `config/config.json`).

### POST /api/restart

Restarts the server process.

### GET /dashboard

Serves the monitoring dashboard UI.

## Response headers

Mnemosyne adds a `X-Cache-Status` header to SSE responses:

| Value | Meaning |
|---|---|
| `HIT` | System prompt was found in cache (L1 warm or L2 cold); KV state was restored from disk |
| `MISS` | First request for this system prompt; KV state was saved after streaming |

## Logging

All operations are logged to the terminal with color-coded tags:

- **■ CACHE HIT** (green) — KV state restored from L1 RAMDisk
- **■ COLD HIT** (yellow) — KV state copied L2→L1 then restored
- **■ CACHE MISS** (yellow) — Full request proxied, cache save queued
- **SLOT ACQUIRED / QUEUED / PASSED / RELEASED** — Concurrency queue status
- **EVICT** — L1 eviction activity (threshold exceeded, files deleted)
- **GC Starting sweep / DELETED / Sweep complete** — Garbage collector activity
- **SAVE** — Background KV cache save operation (L1 + L2 persist)
- **RESTORE** — KV cache restore operation
- **WARMUP** — Pre-warm activity on startup
- **✖ ERROR** (red) — Errors and failures

## Project structure

```
server.js              # Single-file Fastify server with all logic
dashboard.html         # Real-time monitoring dashboard
ghost_detector.txt     # Request logging for debugging
slot_cache/            # L2 SSD — KV cache .bin files (persistent)
ramdisk_cache/         # L1 RAMDisk — KV cache .bin files (volatile)
config/                # Persistent settings
  config.json          # Current settings
  config.json.bak      # Backup
settings.js            # Settings loader
package.json           # Dependencies
```

## License
