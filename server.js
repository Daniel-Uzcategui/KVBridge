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

const { copyFile, rm, readdir, stat, mkdir, appendFile, readFile } = require('fs/promises');
const { existsSync } = require('fs');
const { join, extname } = require('path');

// ─── Tiered Storage Configuration ───────────────────────────────────

const L1_DIR = join('/home/daniel', 'Models', 'ramdisk_cache');  // RAMDisk — volatile
const L2_DIR = join('/home/daniel', 'Models', 'slot_cache');      // SSD — persistent

// Concurrency
const MAX_CONCURRENT_SLOTS = 2;

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
  magenta:'\x1b[35m',
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
// Serial log writer — prevents interleaving under concurrent load.
// Each call enqueues a log entry; a single background drain serializes
// writes to disk so no two appendFile calls overlap.

const logQueue = [];
let logDraining = false;

async function flushLogQueue() {
  if (logDraining || logQueue.length === 0) return;
  logDraining = true;

  try {
    while (logQueue.length > 0) {
      const entry = logQueue.shift();
      await appendFile(join(__dirname, 'ghost_detector.txt'), entry, 'utf8');
    }
  } finally {
    logDraining = false;
  }
}

function enqueueLogEntry(logContent) {
  logQueue.push(logContent);
  flushLogQueue().catch(err => {
    logWarn(`Log flush failed: ${err.message}`);
  });
}

function logRequestToFile(reqId, systemPrompt, firstUserMessage) {
  const timestamp = new Date().toISOString();
  let logContent = `\n========== [${timestamp}] REQ: ${reqId} ==========\n`;
  logContent += `[SYSTEM PROMPT]\n${systemPrompt ? systemPrompt.substring(0, 800) + '...' : 'NO SYSTEM PROMPT'}\n`;

  if (firstUserMessage) {
    logContent += `\n[FIRST USER MSG]\n${firstUserMessage.content ? firstUserMessage.content.substring(0, 500) + '...' : 'NONE'}\n`;
  }
  logContent += `======================================================\n`;

  enqueueLogEntry(logContent);
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
// Now mapped by slot ID to avoid race conditions with MAX_CONCURRENT_SLOTS > 1
const llamaSlotHashes = new Array(MAX_CONCURRENT_SLOTS).fill(null);

const availableSlots = Array.from({ length: MAX_CONCURRENT_SLOTS }, (_, i) => i);
const activeSlots = new Map(); // reqId -> slotId

function acquireSlot(reqId, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      const err = new Error('aborted');
      err.name = 'AbortError';
      return reject(err);
    }

    if (availableSlots.length > 0) {
      const slotId = availableSlots.shift();
      activeSlots.set(reqId, slotId);
      log('SLOT', `${C.green}ACQUIRED${C.reset}  slot=${slotId}  ${reqId}`);
      resolve(slotId);
    } else {
      log('SLOT', `${C.yellow}QUEUED${C.reset}  queue=${slotQueue.length + 1}  ${reqId}`);
      slotQueue.push({ resolve, reject, reqId, signal });
    }
  });
}

function releaseSlot(reqId) {
  // 1. Is it waiting in the queue?
  const queueIndex = slotQueue.findIndex(item => item.reqId === reqId);
  if (queueIndex !== -1) {
    const [removed] = slotQueue.splice(queueIndex, 1);
    const err = new Error('aborted');
    err.name = 'AbortError';
    removed.reject(err);
    log('SLOT', `${C.yellow}REMOVED FROM QUEUE${C.reset}  ${reqId}`);
    return;
  }

  // 2. Otherwise, it holds an active slot.
  const slotId = activeSlots.get(reqId);
  if (slotId !== undefined && slotId !== null) {
    activeSlots.delete(reqId);
    
    // Try to pass the slot directly to the next non-aborted queued request
    while (slotQueue.length > 0) {
      const next = slotQueue.shift();
      if (!next.signal?.aborted) {
        activeSlots.set(next.reqId, slotId);
        log('SLOT', `${C.green}PASSED${C.reset}  slot=${slotId}  ${reqId} -> ${next.reqId}`);
        next.resolve(slotId);
        return;
      } else {
        const err = new Error('aborted');
        err.name = 'AbortError';
        next.reject(err);
      }
    }
    
    // No valid queued requests, release the slot
    availableSlots.push(slotId);
    log('SLOT', `${C.green}RELEASED${C.reset}  slot=${slotId}  ${reqId}`);
  }
}

// Global deduplication map: hash -> Array of resolver functions
const inFlightMisses = new Map();

// ─── Active cache file tracking — prevents GC from deleting files in use ─

const activeCacheFiles = new Set();  // Set of hashes currently being accessed

function markCacheActive(hash) {
  activeCacheFiles.add(hash);
}

function markCacheInactive(hash) {
  activeCacheFiles.delete(hash);
}

// ─── 2. L1 Eviction Sweeper ─────────────────────────────────────────

async function sweepL1Eviction() {
  try {
    const files = await safeReaddir(L1_DIR);
    const binFiles = files.filter(f => extname(f) === '.bin');

    // Calculate total L1 size — skip files currently in use
    let totalSize = 0;
    const fileInfo = [];
    for (const f of binFiles) {
      const hash = f.replace(/\.bin$/, '');
      if (activeCacheFiles.has(hash)) continue;

      const s = await safeStat(join(L1_DIR, f));
      if (s) {
        totalSize += s.size;
        fileInfo.push({ name: f, size: s.size, mtimeMs: s.mtimeMs, hash });
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
      // Race guard: skip if file became active since we scanned
      if (activeCacheFiles.has(fi.hash)) continue;
      const fpath = join(L1_DIR, fi.name);
      // Existence double-check before delete (second race guard)
      if (!existsSync(fpath)) continue;
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
      const hash = f.replace(/\.bin$/, '');
      // Skip files currently in use by a route handler
      if (activeCacheFiles.has(hash)) continue;

      const s = await safeStat(join(L1_DIR, f));
      if (s && now - s.mtimeMs > MAX_FILE_AGE_MS) {
        const fpath = join(L1_DIR, f);
        // Existence double-check before delete (race guard)
        if (!existsSync(fpath)) continue;
        await rm(fpath);
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
async function saveKVCache(hash, model, slotId) {
  const filename = `${hash}.bin`;

  try {
    log('SAVE', `Starting  →  ${filename}`);

    const res = await llamaPost(`/slots/${slotId}?action=save`, { filename, model });
    await res.text();

    log('SAVE', `${C.green}Saved${C.reset}  →  L1 RAMDisk (Slot ${slotId})`);
    return { filename, cacheDir: L1_DIR };
  } catch (err) {
    logErr(`Save failed for ${hash}.bin (Slot ${slotId}): ${err.message}`);
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
async function restoreKVCache(hash, model, cacheDir, slotId) {
  const filename = `${hash}.bin`;

  log('RESTORE', `Loading  →  ${filename}  (from L1 RAMDisk)`);

  try {
    const res = await llamaPost(`/slots/${slotId}?action=restore`, { filename, model });
    log('RESTORE', `${C.green}Success${C.reset}  →  ${filename}  (slot ${slotId}, ${res.status})`);
    return true;
  } catch (err) {
    logErr(`Restore failed for ${hash}.bin (Slot ${slotId}): ${err.message}`);
    throw err;
  }
}

// ─── 5. SSE Streaming Proxy ────────────────────────────────────────

async function proxyStream(llamaRes, reply, signal) {
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
      while (!signal.aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        if (signal.aborted) break;
        reply.raw.write(value);
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        logErr(`Stream read error: ${err.message}`);
      }
    } finally {
      reader.releaseLock();
      if (!reply.raw.writableEnded) {
        reply.raw.end();
      }
    }
  } else {
    reply.raw.end();
  }
}

async function proxyStreamWithHeaders(llamaRes, reply, extraHeaders, signal) {
  reply.hijack();

  reply.raw.writeHead(llamaRes.status, extraHeaders);

  const reader = llamaRes.body?.getReader();
  if (reader) {
    try {
      while (!signal.aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        if (signal.aborted) break;
        reply.raw.write(value);
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        logErr(`Stream read error: ${err.message}`);
      }
    } finally {
      reader.releaseLock();
      if (!reply.raw.writableEnded) {
        reply.raw.end();
      }
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

function computeCacheHash(model, systemPrompt, firstUserMessage) {
  const modelPrefix = model ? `${model}_` : '';
  let hashInput = `${modelPrefix}${systemPrompt}`;

  if (firstUserMessage && typeof firstUserMessage.content === 'string') {
    if (firstUserMessage.content.length > RAG_CONTENT_LENGTH_THRESHOLD) {
      hashInput = `${modelPrefix}${systemPrompt}\n\n${firstUserMessage.content}`;
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

server.get('/dashboard', async (request, reply) => {
  try {
    const html = await readFile(join(__dirname, 'dashboard.html'), 'utf8');
    reply.type('text/html').send(html);
  } catch (e) {
    reply.status(500).send('Dashboard UI not found');
  }
});

server.get('/api/stats', async (request, reply) => {
  let l1UsageBytes = 0;
  try {
    const files = await safeReaddir(L1_DIR);
    for (const f of files) {
      if (extname(f) === '.bin') {
        const s = await safeStat(join(L1_DIR, f));
        if (s) l1UsageBytes += s.size;
      }
    }
  } catch (e) {}

  let llamaMetrics = {};
  let llamaPort = LLAMA_PORT;
  try {
    const { execSync } = require('child_process');
    const out = execSync(`ps -ef | grep llama-server | grep -v grep | grep -v " ${LLAMA_PORT} "`).toString();
    const match = out.match(/--port\s+(\d+)/);
    if (match) llamaPort = match[1];
  } catch(e) {}

  try {
    const mRes = await fetch(`http://${LLAMA_HOST}:${llamaPort}/metrics`);
    if (mRes.ok) {
      const mText = await mRes.text();
      mText.split('\n').forEach(line => {
        if (!line.startsWith('#') && line.includes(' ')) {
          const [key, val] = line.split(' ');
          const num = parseFloat(val);
          if (!isNaN(num)) llamaMetrics[key] = num;
        }
      });
    }
  } catch(e) {}

  const slotsInfo = [];
  for (let i = 0; i < MAX_CONCURRENT_SLOTS; i++) {
    let activeReq = null;
    for (const [rId, sId] of activeSlots.entries()) {
      if (sId === i) { activeReq = rId; break; }
    }
    slotsInfo.push({
      id: i,
      hash: llamaSlotHashes[i],
      activeReq
    });
  }

  reply.send({
    queueLength: slotQueue.length,
    l1UsageBytes,
    slots: slotsInfo,
    llamaMetrics
  });
});

server.post('/v1/chat/completions', async (request, reply) => {
  const body = request.body;
  const reqId = (body?.metadata && body.metadata?.request_id)
    || body?.id
    || request.id;

  // Extract system prompt + RAG context
  const { systemPrompt, firstUserMessage, userMessages } = extractSystemPrompt(body);
  logRequestToFile(reqId, systemPrompt, firstUserMessage);

  const model = body?.model || null;
  const hash = systemPrompt ? computeCacheHash(model, systemPrompt, firstUserMessage) : null;

  // ─── AbortController + disconnect handler (Fix #1: Zombie Slots) ─
  const abortController = new AbortController();
  const { signal } = abortController;

  let slotReleased = false;
  let assignedSlotId = null;
  let resolveInFlight = null;

  const safeReleaseSlot = () => {
    if (!slotReleased) {
      slotReleased = true;
      releaseSlot(reqId);
      
      if (resolveInFlight) resolveInFlight();
    }
  };

  request.raw.on('aborted', () => {
    if (!reply.raw.writableEnded) {
      log('DISCONNECT', `Client dropped  ${reqId}`);
      abortController.abort();
      safeReleaseSlot();
    }
  });

  // No system prompt → direct proxy (no caching)
  if (!hash) {
    log('PROXY', `No system prompt — forwarding directly (id=${reqId})`);

    try {
      assignedSlotId = await acquireSlot(reqId, signal);
      const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
        },
        body: JSON.stringify({ ...body, id_slot: assignedSlotId }),
        signal,
      });

      await proxyStream(llamaRes, reply, signal);
    } catch (err) {
      if (err.name !== 'AbortError') throw err;
    } finally {
      safeReleaseSlot();
    }
    return;
  }

  const filename = `${hash}.bin`;
  const l1Path = join(L1_DIR, filename);
  const l2Path = join(L2_DIR, filename);

  // ──────────────────────────────────────────────────────────────
  // DEDUPLICATION — Wait if someone else is creating this cache
  // ──────────────────────────────────────────────────────────────
  if (inFlightMisses.has(hash)) {
    log('DEDUP', `Waiting for in-flight cache creation for ${hash.slice(0, 8)}`);
    try {
      await new Promise((resolve, reject) => {
        const onAbort = () => {
          const err = new Error('aborted');
          err.name = 'AbortError';
          reject(err);
        };
        if (signal.aborted) return onAbort();
        signal.addEventListener('abort', onAbort);
        
        inFlightMisses.get(hash).push(() => {
          signal.removeEventListener('abort', onAbort);
          resolve();
        });
      });
    } catch (err) {
      if (err.name === 'AbortError') return;
      throw err;
    }
  }

  const l1Exists = existsSync(l1Path);
  const l2Exists = existsSync(l2Path);

  // ──────────────────────────────────────────────────────────────
  // CACHE HIT — L1 warm hit: restore immediately from RAMDisk
  // ──────────────────────────────────────────────────────────────
  if (l1Exists) {
    markCacheActive(hash);
    try {
      logHit(hash);

      try {
        assignedSlotId = await acquireSlot(reqId, signal);
        try {
          if (llamaSlotHashes[assignedSlotId] !== hash) {
            await restoreKVCache(hash, model, cachedCacheDir || L1_DIR, assignedSlotId);
            llamaSlotHashes[assignedSlotId] = hash;
            log('INFO', `KV cache restored for ${hash.slice(0, 12)} into slot ${assignedSlotId}`);
          } else {
            log('INFO', `KV cache already in VRAM slot ${assignedSlotId} for ${hash.slice(0, 12)}... Skipping restore.`);
          }
        } catch (err) {
          log('WARN', `Restore failed (${err.message}) — falling back to full request`);
        }

        // Proxy with only user messages (system prompt already in KV cache)
        const proxyBody = { ...body, messages: userMessages, id_slot: assignedSlotId };
        const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
          },
          body: JSON.stringify(proxyBody),
          signal,
        });

        await proxyStreamWithHeaders(llamaRes, reply, {
          'Content-Type':      'text/event-stream',
          'Cache-Control':     'no-cache',
          'Connection':        'keep-alive',
          'X-Accel-Buffering': 'no',
          'X-Cache-Status':    'HIT',
        }, signal);
      } finally {
        safeReleaseSlot();
      }
    } finally {
      markCacheInactive(hash);
    }
    return;
  }

  // ──────────────────────────────────────────────────────────────
  // COLD HIT — L2→L1 copy, then restore from RAMDisk
  // ──────────────────────────────────────────────────────────────
  if (l2Exists) {
    markCacheActive(hash);
    try {
      logColdHit(hash);

      // Copy from L2 (SSD) to L1 (RAMDisk) — awaited so file is ready
      try {
        await copyFileL2ToL1(hash);
      } catch (err) {
        logWarn(`L2→L1 copy failed (${err.message}) — falling back to full request`);
      }

      try {
        assignedSlotId = await acquireSlot(reqId, signal);
        try {
          if (llamaSlotHashes[assignedSlotId] !== hash) {
            await restoreKVCache(hash, model, cachedCacheDir || L1_DIR, assignedSlotId);
            llamaSlotHashes[assignedSlotId] = hash;
            log('INFO', `KV cache restored for ${hash.slice(0, 12)} into slot ${assignedSlotId}`);
          } else {
            log('INFO', `KV cache already in VRAM slot ${assignedSlotId} for ${hash.slice(0, 12)}... Skipping restore.`);
          }
        } catch (err) {
          log('WARN', `Restore failed (${err.message}) — falling back to full request`);
        }

        const proxyBody = { ...body, messages: userMessages, id_slot: assignedSlotId };
        const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
          },
          body: JSON.stringify(proxyBody),
          signal,
        });

        await proxyStreamWithHeaders(llamaRes, reply, {
          'Content-Type':      'text/event-stream',
          'Cache-Control':     'no-cache',
          'Connection':        'keep-alive',
          'X-Accel-Buffering': 'no',
          'X-Cache-Status':    'HIT',
        }, signal);
      } finally {
        safeReleaseSlot();
      }
    } finally {
      markCacheInactive(hash);
    }
    return;
  }

  // ──────────────────────────────────────────────────────────────
  // CACHE MISS — proxy full request, save to L1, persist to L2
  // ──────────────────────────────────────────────────────────────
  markCacheActive(hash);
  try {
    logMiss(hash);

    // Lock deduplication
    inFlightMisses.set(hash, []);
    resolveInFlight = () => {
      const waiters = inFlightMisses.get(hash) || [];
      inFlightMisses.delete(hash);
      waiters.forEach(r => r());
    };

    try {
      assignedSlotId = await acquireSlot(reqId, signal);
      const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
        },
        body: JSON.stringify({ ...body, id_slot: assignedSlotId }),
        signal,
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
            while (!signal.aborted) {
              const { done, value } = await reader.read();
              if (done) break;
              if (signal.aborted) break;
              reply.raw.write(value);
            }
          } catch (err) {
            if (err.name !== 'AbortError') {
              logErr(`Stream read error: ${err.message}`);
            }
          } finally {
            reader.releaseLock();
            if (!reply.raw.writableEnded) {
              reply.raw.end();
            }
          }
        };

        // We MUST await the pump and save to keep the Node route alive 
        // and prevent the slot from being released prematurely.
        try {
          await pump();
          const saveResult = await saveKVCache(hash, model, assignedSlotId);
          if (saveResult) {
            llamaSlotHashes[assignedSlotId] = hash;
            cachedCacheDir = saveResult.cacheDir;
            const l2Path2 = join(L2_DIR, `${hash}.bin`);
            // Fire and forget the L2 copy, since it doesn't need the Llama slot
            copyFile(join(L1_DIR, `${hash}.bin`), l2Path2).then(() => {
              log('SAVE', `${C.green}Persisted${C.reset}  →  L2 SSD`);
            }).catch(err => logWarn(`L2 persist failed: ${err.message}`));
          }
        } catch (err) {
          logWarn(`Save pipeline failed: ${err.message}`);
        }
      } else {
        reply.raw.end();
        try {
          const saveResult = await saveKVCache(hash, model, assignedSlotId);
          if (saveResult) {
            llamaSlotHashes[assignedSlotId] = hash;
            cachedCacheDir = saveResult.cacheDir;
            const l2Path2 = join(L2_DIR, `${hash}.bin`);
            copyFile(join(L1_DIR, `${hash}.bin`), l2Path2).then(() => {
              log('SAVE', `${C.green}Persisted${C.reset}  →  L2 SSD`);
            }).catch(err => logWarn(`L2 persist failed: ${err.message}`));
          }
        } catch (err) {
          logWarn(`Save pipeline failed: ${err.message}`);
        }
      }
    } finally {
      safeReleaseSlot();
    }
  } finally {
    markCacheInactive(hash);
  }

  return;
});

// ─── 8. Error Handler ──────────────────────────────────────────────

server.setErrorHandler((error, request, reply) => {
  // IGNORE ABORT ERRORS
  if (error.name === 'AbortError' || error.message.includes('aborted')) {
    return; // Client disconnected, nothing to do.
  }

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

    const PORT = 8080;
    const stripAnsi = (str) => str.replace(/\x1b\[[0-9;]*m/g, '');
    const lines = [
      `  ${C.bold}${C.green}KV Cache Semantic Router — Tiered Storage${C.reset}`,
      `  Listening on  ${C.bold}http://0.0.0.0:${PORT}${C.reset}`,
      `  Dashboard UI  ${C.bold}${C.magenta}http://127.0.0.1:${PORT}/dashboard${C.reset}`,
      `  Proxy target  ${C.bold}${LLAMA_BASE}${C.reset}`,
      `  L1 (RAMDisk)  ${C.bold}${L1_DIR}${C.reset} (tmpfs)`,
      `  L2 (SSD)      ${C.bold}${L2_DIR}${C.reset} (persistent)`,
      `  Max slots     ${C.bold}${MAX_CONCURRENT_SLOTS}${C.reset}`,
      `  Evict >       ${C.bold}${(L1_EVICT_THRESHOLD / 1024 / 1024 / 1024).toFixed(1)}GB${C.reset} down to ${C.bold}${(L1_EVICT_TARGET / 1024 / 1024 / 1024).toFixed(1)}GB${C.reset}`,
      `  GC interval   ${C.bold}${GARBAGE_COLLECT_INTERVAL_MS / 60000}min${C.reset} maxAge ${C.bold}${MAX_FILE_AGE_MS / 3600000}h${C.reset}`,
      `  RAG threshold ${C.bold}${RAG_CONTENT_LENGTH_THRESHOLD} chars${C.reset}`
    ];

    const boxWidth = 68;
    console.log(`\n${C.bold}${C.cyan}╔${'═'.repeat(boxWidth)}╗${C.reset}`);
    for (const line of lines) {
      const visibleLen = stripAnsi(line).length;
      const padding = Math.max(0, boxWidth - visibleLen);
      console.log(`${C.bold}${C.cyan}║${C.reset}${line}${' '.repeat(padding)}${C.bold}${C.cyan}║${C.reset}`);
    }
    console.log(`${C.bold}${C.cyan}╚${'═'.repeat(boxWidth)}╝${C.reset}\n`);
  } catch (err) {
    logErr(`Cannot start server: ${err.message}`);
    process.exit(1);
  }
};

start();
