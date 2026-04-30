#!/usr/bin/env node
/**
 * KV Cache Semantic Router — Tiered Storage (L1 RAM / L2 SSD)
 * ============================================================
 * Storage Orchestrator — never holds binary data in V8 heap.
 *
 * L1_DIR (RAMDisk, tmpfs):  /home/daniel/Models/ramdisk_cache  — volatile, fast
 * L2_DIR (SSD, persistent): /home/daniel/Models/slot_cache     — unlimited, slow
 *
 * Usage:
 *   node server.js
 *
 * Listens on :8080, proxies to llama.cpp at :11434.
 */

'use strict';

const { createHash } = require('crypto');
const fastify = require('fastify');
const fastifyCors = require('@fastify/cors');
const fastifyFormBody = require('@fastify/formbody');

const { copyFile, rm, readdir, stat, mkdir, appendFile } = require('fs/promises');
const { existsSync } = require('fs');
const { join, extname } = require('path');

// ─── Tiered Storage Configuration ───────────────────────────────────

const L1_DIR = join('/home/daniel', 'Models', 'ramdisk_cache');  // RAMDisk — volatile
const L2_DIR = join('/home/daniel', 'Models', 'slot_cache');      // SSD — persistent

// Concurrency
const MAX_CONCURRENT_SLOTS = 3;

// Eviction thresholds (bytes)
const L1_EVICT_THRESHOLD = 2_684_354_560;  // 2.5 GB — evict when exceeded
const L1_EVICT_TARGET    = 2_147_483_648;  // 2.0 GB — evict down to this

// GC sweep interval
const GARBAGE_COLLECT_INTERVAL_MS = 5 * 60 * 1000;  // 5 min
const MAX_FILE_AGE_MS = 12 * 60 * 60 * 1000;         // 12 hours

// RAG prefix caching
const RAG_CONTENT_LENGTH_THRESHOLD = 1000;

// Warm-up (pre-warm on startup)
const WARMUP_TOP_N = 15;

// ─── Helpers ────────────────────────────────────────────────────────

const C = {
  reset:  '\x1b[0m',
  bold:   '\x1b[1m',
  grey:   '\x1b[90m',
  green:  '\x1b[32m',
  yellow: '\x1b[33m',
  red:    '\x1b[31m',
  cyan:   '\x1b[36m',
};

function log(tag, msg) {
  console.log(`${C.grey}${new Date().toISOString().slice(11, 23)}${C.reset}  ${C.bold}[${tag}]${C.reset} ${msg}`);
}

function logHit(hash) {
  console.log(`${C.green}${C.bold}■ CACHE HIT${C.reset}  slot  →  ${hash}.bin  (restoring from L1 RAMDisk)`);
}

function logColdHit(hash) {
  console.log(
    `${C.yellow}${C.bold}■ COLD HIT${C.reset}  slot  →  ${hash}.bin  (copying L2→L1, then restoring)`
  );
}

function logMiss(hash) {
  console.log(
    `${C.yellow}${C.bold}■ CACHE MISS${C.reset}  slot  →  ${hash}.bin  (will save to L1, then persist to L2)`
  );
}

function logWarn(msg) {
  console.log(`${C.yellow}${C.bold}⚠ WARN${C.reset}    ${msg}`);
}

function logErr(msg) {
  console.error(`${C.red}${C.bold}✖ ERROR${C.reset}   ${msg}`);
}

// ─── Ghost Detector Logger ──────────────────────────────────────────
async function logRequestToFile(reqId, systemPrompt, firstUserMessage) {
  try {
    const timestamp = new Date().toISOString();
    let logContent = `\n========== [${timestamp}] REQ: ${reqId} ==========\n`;
    logContent += `[SYSTEM PROMPT]\n${systemPrompt ? systemPrompt.substring(0, 800) + '...' : 'NO SYSTEM PROMPT'}\n`;
    
    if (firstUserMessage) {
      logContent += `\n[FIRST USER MSG]\n${firstUserMessage.content ? firstUserMessage.content.substring(0, 500) + '...' : 'NONE'}\n`;
    }
    logContent += `======================================================\n`;
    
    // Escribe el log en el disco de forma asíncrona (no bloquea el Event Loop)
    await appendFile(join(__dirname, 'ghost_detector.txt'), logContent, 'utf8');
  } catch (err) {
    logWarn(`No se pudo escribir el ghost log: ${err.message}`);
  }
}
// ─── fs/promises convenience wrappers ───────────────────────────────

async function safeReaddir(dir) {
  try {
    return await readdir(dir);
  } catch {
    return [];
  }
}

async function safeStat(path) {
  try {
    return await stat(path);
  } catch {
    return null;
  }
}

// ─── 1. Slot Semaphore (FIFO Concurrency Queue) ─────────────────────

let currentSlots = 0;
const slotQueue = [];

// Stores the llama.cpp cache directory path after first save (needed for restore)
let cachedCacheDir = null;

// Tracks the most recently loaded hash to avoid redundant restores in the same process lifetime
let lastLoadedHash = null;

function acquireSlot(reqId) {
  return new Promise((resolve, reject) => {
    if (currentSlots < MAX_CONCURRENT_SLOTS) {
      currentSlots++;
      log('SLOT', `${C.green}ACQUIRED${C.reset}  (${currentSlots}/${MAX_CONCURRENT_SLOTS})  ${reqId}`);
      resolve();
    } else {
      log('SLOT', `${C.yellow}QUEUED${C.reset}  queue=${slotQueue.length + 1}  (${currentSlots}/${MAX_CONCURRENT_SLOTS})  ${reqId}`);
      slotQueue.push({ resolve, reject, reqId });
    }
  });
}

function releaseSlot(reqId) {
  if (slotQueue.length > 0) {
    const { resolve } = slotQueue.shift();
    log('SLOT', `${C.green}PASSED${C.reset}  queue=${slotQueue.length}  (${currentSlots}/${MAX_CONCURRENT_SLOTS})  ${reqId}`);
    resolve();
  } else {
    currentSlots--;
    log('SLOT', `${C.green}RELEASED${C.reset}  (${currentSlots}/${MAX_CONCURRENT_SLOTS})  ${reqId}`);
  }
}

// ─── 2. L1 Eviction Sweeper ─────────────────────────────────────────

async function sweepL1Eviction() {
  try {
    const files = await safeReaddir(L1_DIR);
    const binFiles = files.filter(f => extname(f) === '.bin');

    // Calculate total L1 size
    let totalSize = 0;
    const fileInfo = [];
    for (const f of binFiles) {
      const s = await safeStat(join(L1_DIR, f));
      if (s) {
        totalSize += s.size;
        fileInfo.push({ name: f, size: s.size, mtimeMs: s.mtimeMs });
      }
    }

    if (totalSize <= L1_EVICT_THRESHOLD) {
      return;  // Under threshold, nothing to do
    }

    log('EVICT', `${C.bold}L1 over threshold${C.reset}  ${totalSize / 1024 / 1024 / 1024.0}GB > ${L1_EVICT_THRESHOLD / 1024 / 1024 / 1024.0}GB`);

    // Sort oldest first, evict until under target
    fileInfo.sort((a, b) => a.mtimeMs - b.mtimeMs);

    let evicted = 0;
    let freed = 0;
    for (const fi of fileInfo) {
      if (totalSize <= L1_EVICT_TARGET) break;
      const fpath = join(L1_DIR, fi.name);
      await rm(fpath);
      evicted++;
      freed += fi.size;
      totalSize -= fi.size;
      log('EVICT', `  ${C.red}DELETED${C.reset}  ${fi.name}  (${fi.size / 1024 / 1024}KB)`);
    }

    log('EVICT', `${C.green}Sweep done${C.reset}  evicted=${evicted}  freed=${freed / 1024 / 1024}MB  remaining=${totalSize / 1024 / 1024 / 1024.0}GB`);
  } catch (err) {
    logErr(`L1 eviction sweep failed: ${err.message}`);
  }
}

// LRU GC sweeper (L1 only — deletes old files regardless of size)
async function gcSweep() {
  log('GC', `${C.bold}Starting sweep${C.reset}  (max age=${(MAX_FILE_AGE_MS / 3600000).toFixed(0)}h)`);
  try {
    const files = await safeReaddir(L1_DIR);
    const binFiles = files.filter(f => extname(f) === '.bin');
    const now = Date.now();
    let deleted = 0;

    for (const f of binFiles) {
      const s = await safeStat(join(L1_DIR, f));
      if (s && now - s.mtimeMs > MAX_FILE_AGE_MS) {
        await rm(join(L1_DIR, f));
        deleted++;
        log('GC', `  ${C.red}DELETED${C.reset}  ${f}  (age=${Math.round((now - s.mtimeMs) / 3600000)}h)`);
      }
    }

    if (deleted > 0) {
      log('GC', `${C.green}Sweep done${C.reset}  deleted=${deleted}`);
    }
  } catch (err) {
    logErr(`GC sweep failed: ${err.message}`);
  }
}

// ─── 3. File Copy Utilities ─────────────────────────────────────────

async function copyFileL2ToL1(hash) {
  const src = join(L2_DIR, `${hash}.bin`);
  const dst = join(L1_DIR, `${hash}.bin`);
  await copyFile(src, dst);
  log('COPY', `${C.green}L2→L1${C.reset}  ${hash}.bin`);
}

// ─── 4. llama.cpp API Helpers ───────────────────────────────────────

const LLAMA_HOST = '127.0.0.1';
const LLAMA_PORT = 11434;
const LLAMA_BASE = `http://${LLAMA_HOST}:${LLAMA_PORT}`;

async function llamaPost(path, payload) {
  const res = await fetch(`${LLAMA_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '(no body)');
    throw new Error(`llama.cpp ${path} failed ${res.status}: ${text.slice(0, 300)}`);
  }

  return res;
}

/**
 * Save KV cache — llama.cpp saves to its internal cache dir.
 * We copy the result to L1_DIR (RAMDisk), then persist to L2_DIR (SSD) in background.
 *
 * @returns {{ filename: string, cacheDir: string }}
 *   cacheDir = where llama.cpp saved the file (its internal cache dir)
 */
async function saveKVCache(hash, model) {
  const filename = `${hash}.bin`;

  try {
    log('SAVE', `Starting  →  ${filename}`);

    for (let s = 0; s < MAX_CONCURRENT_SLOTS; s++) {
      try {
        // Mandamos solo el 'filename' puro (sin ruta absoluta)
        const res = await llamaPost(`/slots/${s}?action=save`, { filename, model });

        // Esperamos a que la promesa se resuelva por completo antes de continuar
        await res.text();

        log('SAVE', `${C.green}Saved${C.reset}  →  L1 RAMDisk (Slot ${s})`);

        // Retornamos el cacheDir correcto
        return { filename, cacheDir: L1_DIR };
      } catch (err) {
        if (s < MAX_CONCURRENT_SLOTS - 1) {
          log('SAVE', `  Slot ${s} unavailable, trying next…`);
          continue;
        }
        throw err;
      }
    }
  } catch (err) {
    logErr(`Save failed for ${hash}.bin: ${err.message}`);
    return null;
  }
}

/**
 * Restore KV cache — copy from L1_DIR to llama.cpp's cache dir,
 * call the restore endpoint, then clean up the temp copy.
 *
 * @param {string} hash - Cache hash
 * @param {string} model - Model name
 * @param {string} cacheDir - Where llama.cpp expects the file (from save response)
 */
async function restoreKVCache(hash, model, cacheDir) {
  const filename = `${hash}.bin`;

  log('RESTORE', `Loading  →  ${filename}  (from L1 RAMDisk)`);

  for (let s = 0; s < MAX_CONCURRENT_SLOTS; s++) {
    try {
      // Mandamos solo el 'filename' puro
      const res = await llamaPost(`/slots/${s}?action=restore`, { filename, model });
      log('RESTORE', `${C.green}Success${C.reset}  →  ${filename}  (slot ${s}, ${res.status})`);
      return true;
    } catch (err) {
      if (s < MAX_CONCURRENT_SLOTS - 1) {
        log('RESTORE', `  Slot ${s} unavailable, trying next…`);
        continue;
      }
      throw err;
    }
  }
}

// ─── 5. SSE Streaming Proxy ────────────────────────────────────────

async function proxyStream(llamaRes, reply) {
  reply.hijack();

  reply.raw.writeHead(llamaRes.status, {
    'Content-Type':      'text/event-stream',
    'Cache-Control':     'no-cache',
    'Connection':        'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  const reader = llamaRes.body?.getReader();
  if (reader) {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        reply.raw.write(value);
      }
    } catch {} finally {
      reader.releaseLock();
      reply.raw.end();
    }
  } else {
    reply.raw.end();
  }
}

async function proxyStreamWithHeaders(llamaRes, reply, extraHeaders) {
  reply.hijack();

  reply.raw.writeHead(llamaRes.status, extraHeaders);

  const reader = llamaRes.body?.getReader();
  if (reader) {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        reply.raw.write(value);
      }
    } catch {} finally {
      reader.releaseLock();
      reply.raw.end();
    }
  } else {
    reply.raw.end();
  }
}

// ─── 6. Request Processing ─────────────────────────────────────────

function extractSystemPrompt(body) {
  const messages = Array.isArray(body?.messages) ? body.messages : [];
  let systemPrompt = '';
  const userMessages = [];
  let firstUserMessage = null;

  let systemFound = false;
  for (const msg of messages) {
    if (!systemFound && msg.role === 'system' && msg.content) {
      systemPrompt = typeof msg.content === 'string'
        ? msg.content
        : JSON.stringify(msg.content);
      systemFound = true;
    } else if (msg.role === 'user' && msg.content && firstUserMessage === null) {
      firstUserMessage = {
        role: 'user',
        content: typeof msg.content === 'string'
          ? msg.content
          : JSON.stringify(msg.content),
      };
      userMessages.push(msg);
    } else {
      userMessages.push(msg);
    }
  }

  return { systemPrompt, firstUserMessage, userMessages };
}

function computeCacheHash(systemPrompt, firstUserMessage) {
  let hashInput = systemPrompt;

  if (firstUserMessage && typeof firstUserMessage.content === 'string') {
    if (firstUserMessage.content.length > RAG_CONTENT_LENGTH_THRESHOLD) {
      hashInput = `${systemPrompt}\n\n${firstUserMessage.content}`;
    }
  }

  return createHash('sha256').update(hashInput, 'utf8').digest('hex');
}

// ─── 9. Server Startup & Pre-Warm ──────────────────────────────────

/**
 * Pre-warm L1 (RAMDisk) from L2 (SSD) on startup.
 * Copies the N most recently modified .bin files.
 */
async function warmupL1() {
  try {
    const files = await safeReaddir(L2_DIR);
    const binFiles = files.filter(f => extname(f) === '.bin');

    if (binFiles.length === 0) {
      log('WARMUP', `${C.green}No .bin files in L2 — cache is cold${C.reset}`);
      return;
    }

    // Sort by mtime descending (most recent first)
    const stats = [];
    for (const f of binFiles) {
      const s = await safeStat(join(L2_DIR, f));
      if (s) stats.push({ name: f, mtimeMs: s.mtimeMs });
    }
    stats.sort((a, b) => b.mtimeMs - a.mtimeMs);

    const toCopy = stats.slice(0, WARMUP_TOP_N);
    let copied = 0;

    for (const item of toCopy) {
      try {
        await copyFile(join(L2_DIR, item.name), join(L1_DIR, item.name));
        copied++;
      } catch (err) {
        logWarn(`Warmup copy failed for ${item.name}: ${err.message}`);
      }
    }

    log('WARMUP', `${C.green}Pre-warmed${C.reset}  ${copied}/${stats.length} files → L1 (${L1_DIR})`);
  } catch (err) {
    logWarn(`Warmup failed: ${err.message}`);
  }
}

// ─── Fastify Server ─────────────────────────────────────────────────

const server = fastify({
  bodyLimit: 50 * 1024 * 1024,   // 50 MB
  maxParamLength: 1024 * 1024,
  disableRequestLogging: true,
});

server.register(fastifyCors, { origin: true });
server.register(fastifyFormBody);

// ─── 7. Route Handler ──────────────────────────────────────────────

server.post('/v1/chat/completions', async (request, reply) => {
  const body = request.body;
  const reqId = (body?.metadata && body.metadata?.request_id)
    || body?.id
    || '????';

  // Extract system prompt + RAG context
  const { systemPrompt, firstUserMessage, userMessages } = extractSystemPrompt(body);
  logRequestToFile(reqId, systemPrompt, firstUserMessage);

  const hash = systemPrompt ? computeCacheHash(systemPrompt, firstUserMessage) : null;
  const model = body?.model || null;

  // No system prompt → direct proxy (no caching)
  if (!hash) {
    log('PROXY', `No system prompt — forwarding directly (id=${reqId})`);

    await acquireSlot(reqId);
    try {
      const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
        },
        body: JSON.stringify(body),
      });

      await proxyStream(llamaRes, reply);
    } finally {
      releaseSlot(reqId);
    }
    return;
  }

  const filename = `${hash}.bin`;
  const l1Path = join(L1_DIR, filename);
  const l2Path = join(L2_DIR, filename);

  const l1Exists = existsSync(l1Path);
  const l2Exists = existsSync(l2Path);

  // ──────────────────────────────────────────────────────────────
  // CACHE HIT — L1 warm hit: restore immediately from RAMDisk
  // ──────────────────────────────────────────────────────────────
  if (l1Exists) {
    logHit(hash);

    await acquireSlot(reqId);
    try {
      try {
        if (lastLoadedHash !== hash) {
          await restoreKVCache(hash, model, cachedCacheDir || L1_DIR);
          lastLoadedHash = hash;
          log('INFO', `KV cache restored for ${hash.slice(0, 12)}...`);
        } else {
          log('INFO', `KV cache already in VRAM for ${hash.slice(0, 12)}... Skipping restore.`);
        }
      } catch (err) {
        log('WARN', `Restore failed (${err.message}) — falling back to full request`);
      }

      // Proxy with only user messages (system prompt already in KV cache)
      const proxyBody = { ...body, messages: userMessages };
      const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
        },
        body: JSON.stringify(proxyBody),
      });

      await proxyStreamWithHeaders(llamaRes, reply, {
        'Content-Type':      'text/event-stream',
        'Cache-Control':     'no-cache',
        'Connection':        'keep-alive',
        'X-Accel-Buffering': 'no',
        'X-Cache-Status':    'HIT',
      });
    } finally {
      releaseSlot(reqId);
    }
    return;
  }

  // ──────────────────────────────────────────────────────────────
  // COLD HIT — L2→L1 copy, then restore from RAMDisk
  // ──────────────────────────────────────────────────────────────
  if (l2Exists) {
    logColdHit(hash);

    // Copy from L2 (SSD) to L1 (RAMDisk) — awaited so file is ready
    try {
      await copyFileL2ToL1(hash);
    } catch (err) {
      logWarn(`L2→L1 copy failed (${err.message}) — falling back to full request`);
    }

    await acquireSlot(reqId);
    try {
      try {
        if (lastLoadedHash !== hash) {
          await restoreKVCache(hash, model, cachedCacheDir || L1_DIR);
          lastLoadedHash = hash;
          log('INFO', `KV cache restored for ${hash.slice(0, 12)}...`);
        } else {
          log('INFO', `KV cache already in VRAM for ${hash.slice(0, 12)}... Skipping restore.`);
        }
      } catch (err) {
        log('WARN', `Restore failed (${err.message}) — falling back to full request`);
      }

      const proxyBody = { ...body, messages: userMessages };
      const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
        },
        body: JSON.stringify(proxyBody),
      });

      await proxyStreamWithHeaders(llamaRes, reply, {
        'Content-Type':      'text/event-stream',
        'Cache-Control':     'no-cache',
        'Connection':        'keep-alive',
        'X-Accel-Buffering': 'no',
        'X-Cache-Status':    'HIT',
      });
    } finally {
      releaseSlot(reqId);
    }
    return;
  }

  // ──────────────────────────────────────────────────────────────
  // CACHE MISS — proxy full request, save to L1, persist to L2
  // ──────────────────────────────────────────────────────────────
  logMiss(hash);

  await acquireSlot(reqId);
  try {
    const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
      },
      body: JSON.stringify(body),
    });

    if (!llamaRes.ok) {
      const errText = await llamaRes.text().catch(() => '');
      logErr(`llama.cpp returned ${llamaRes.status}: ${errText.slice(0, 500)}`);
      throw new Error(`llama.cpp returned ${llamaRes.status}: ${errText.slice(0, 300)}`);
    }

    reply.hijack();
    reply.raw.writeHead(llamaRes.status, {
      'Content-Type':      'text/event-stream',
      'Cache-Control':     'no-cache',
      'Connection':        'keep-alive',
      'X-Accel-Buffering': 'no',
      'X-Cache-Status':    'MISS',
    });

    const reader = llamaRes.body?.getReader();

    if (reader) {
      const pump = async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            reply.raw.write(value);
          }
        } catch {} finally {
          reader.releaseLock();
          reply.raw.end();
        }
      };

      // Save KV cache after stream is consumed, then persist to L2
      (async () => {
        try {
          await pump();
          const saveResult = await saveKVCache(hash, model);
          if (saveResult) {
            lastLoadedHash = hash;
            // Store the cache directory for future restore operations
            cachedCacheDir = saveResult.cacheDir;
            // Copy from L1 (RAMDisk) to L2 (SSD) — fire-and-forget
            const l2Path2 = join(L2_DIR, `${hash}.bin`);
            try {
              await copyFile(join(L1_DIR, `${hash}.bin`), l2Path2);
              log('SAVE', `${C.green}Persisted${C.reset}  →  L2 SSD`);
            } catch (err) {
              logWarn(`L2 persist failed: ${err.message}`);
            }
          }
        } catch (err) {
          logWarn(`Save pipeline failed: ${err.message}`);
        }
      })();
    } else {
      reply.raw.end();
      setImmediate(async () => {
        try {
          const saveResult = await saveKVCache(hash, model);
          if (saveResult) {
            lastLoadedHash = hash;
            cachedCacheDir = saveResult.cacheDir;
            const l2Path2 = join(L2_DIR, `${hash}.bin`);
            await copyFile(join(L1_DIR, `${hash}.bin`), l2Path2);
            log('SAVE', `${C.green}Persisted${C.reset}  →  L2 SSD`);
          }
        } catch (err) {
          logWarn(`Save pipeline failed: ${err.message}`);
        }
      });
    }
  } finally {
    releaseSlot(reqId);
  }

  return;
});

// ─── 8. Error Handler ──────────────────────────────────────────────

server.setErrorHandler((error, request, reply) => {
  logErr(`${error.message}  [${request.id || 'no-id'}]`);
  if (!reply.sent) {
    reply.status(500).send({
      error: {
        message: error.message,
        type: 'internal_error',
      },
    });
  }
});

const start = async () => {
  // Ensure both directories exist
  if (!existsSync(L1_DIR)) {
    mkdir(L1_DIR, { recursive: true });
    log('INIT', `Created L1 (RAMDisk): ${L1_DIR}`);
  }
  if (!existsSync(L2_DIR)) {
    mkdir(L2_DIR, { recursive: true });
    log('INIT', `Created L2 (SSD): ${L2_DIR}`);
  }

  // Pre-warm L1 from L2
  await warmupL1();

  // Start GC sweeper (L1 eviction + age-based cleanup)
  const gcInterval = setInterval(async () => {
    await sweepL1Eviction();
    await gcSweep();
  }, GARBAGE_COLLECT_INTERVAL_MS);
  if (gcInterval.unref) gcInterval.unref();

  try {
    await server.listen({ port: 8080, host: '0.0.0.0' });

    console.log(`
${C.bold}${C.cyan}╔═══════════════════════════════════════════════════════════╗${C.reset}
${C.bold}${C.cyan}║${C.reset}  ${C.bold}${C.green}KV Cache Semantic Router — Tiered Storage${C.reset}              ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Listening on  ${C.bold}http://0.0.0.0:8080${C.reset}                     ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Proxy target  ${C.bold}${LLAMA_BASE}${C.reset}                                    ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  L1 (RAMDisk)  ${C.bold}${L1_DIR}${C.reset}  (3GB tmpfs)                       ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  L2 (SSD)      ${C.bold}${L2_DIR}${C.reset}  (persistent)                       ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Max slots     ${C.bold}${MAX_CONCURRENT_SLOTS}${C.reset}                      ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Evict >       ${C.bold}${L1_EVICT_THRESHOLD / 1024 / 1024 / 1024.0}GB${C.reset}  down to ${C.bold}${L1_EVICT_TARGET / 1024 / 1024 / 1024.0}GB${C.reset}       ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  GC interval   ${C.bold}5min${C.reset}  maxAge ${MAX_FILE_AGE_MS / 3600000}h${C.reset}          ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  RAG threshold ${C.bold}${RAG_CONTENT_LENGTH_THRESHOLD} chars${C.reset}                    ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}╚═══════════════════════════════════════════════════════════╝${C.reset}
`);
  } catch (err) {
    logErr(`Cannot start server: ${err.message}`);
    process.exit(1);
  }
};

start();
