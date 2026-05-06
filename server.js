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
const { spawn } = require('child_process');
const fastify = require('fastify');
const fastifyCors = require('@fastify/cors');
const fastifyFormBody = require('@fastify/formbody');

const { copyFile, rm, readdir, stat, mkdir, appendFile, readFile } = require('fs/promises');
const { existsSync, createWriteStream } = require('fs');
const { join, extname } = require('path');

// ─── Tiered Storage Configuration ───────────────────────────────────

const MODELS_DIR = join('/home/daniel', 'Models');
const L1_DIR = join(MODELS_DIR, 'ramdisk_cache');  // RAMDisk — volatile
const L2_DIR = join(MODELS_DIR, 'slot_cache');      // SSD — persistent
const BACKEND_LOG_DIR = join(__dirname, 'runtime-logs');

// Backend topology
const DEFAULT_BACKEND_MAX_SLOTS = 2;
const BACKEND_HEALTHCHECK_INTERVAL_MS = 5_000;
const BACKEND_HEALTHCHECK_TIMEOUT_MS = 1_500;
const BACKEND_CONFIGS = [
  {
    id: 'llama-a',
    host: '127.0.0.1',
    port: 11434,
    gpuGroup: '0,1',
    maxSlots: DEFAULT_BACKEND_MAX_SLOTS,
    scriptPath: join(MODELS_DIR, 'start_server.sh'),
  },
  {
    id: 'llama-b',
    host: '127.0.0.1',
    port: 11435,
    gpuGroup: '2,3',
    maxSlots: DEFAULT_BACKEND_MAX_SLOTS,
    scriptPath: join(MODELS_DIR, 'start_server2.sh'),
  },
];
const BACKEND_AUTO_START = true;
const BACKEND_STARTUP_GRACE_MS = 12_000;

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

// ─── 1. Backend Registry + Slot State ───────────────────────────────

function createBackendState(config) {
  const baseUrl = `http://${config.host}:${config.port}`;
  return {
    ...config,
    baseUrl,
    healthy: false,
    lastCheckAt: 0,
    lastError: null,
    slotQueue: [],
    availableSlots: Array.from({ length: config.maxSlots }, (_, i) => i),
    activeSlots: new Map(),
    slotContextIndex: new Map(),
    processInfo: {
      status: 'stopped',
      pid: null,
      startedAt: null,
      logPath: join(BACKEND_LOG_DIR, `${config.id}.log`),
      lastExitCode: null,
      lastExitSignal: null,
      lastStartAttemptAt: 0,
      autoStartDisabled: false,
    },
  };
}

const backends = BACKEND_CONFIGS.map(createBackendState);
const backendsById = new Map(backends.map(backend => [backend.id, backend]));
let roundRobinCursor = 0;

const affinityIndex = {
  exact: new Map(),
  system: new Map(),
  prefix: new Map(),
};

function resetBackendRuntimeState(backend) {
  backend.slotQueue.length = 0;
  backend.availableSlots = Array.from({ length: backend.maxSlots }, (_, i) => i);
  backend.activeSlots.clear();
  backend.slotContextIndex.clear();
}

function getBackendProcessSnapshot(backend) {
  return {
    status: backend.processInfo.status,
    pid: backend.processInfo.pid,
    startedAt: backend.processInfo.startedAt,
    logPath: backend.processInfo.logPath,
    lastExitCode: backend.processInfo.lastExitCode,
    lastExitSignal: backend.processInfo.lastExitSignal,
    lastStartAttemptAt: backend.processInfo.lastStartAttemptAt,
    autoStartDisabled: backend.processInfo.autoStartDisabled,
  };
}

function isBackendProcessActive(backend) {
  return backend.processInfo.pid !== null && ['starting', 'running', 'stopping'].includes(backend.processInfo.status);
}

async function tailFile(path, lineCount = 120) {
  if (!existsSync(path)) return '';
  const content = await readFile(path, 'utf8');
  return content.split('\n').slice(-lineCount).join('\n').trim();
}

async function ensureBackendLogFile(backend) {
  if (!existsSync(BACKEND_LOG_DIR)) {
    await mkdir(BACKEND_LOG_DIR, { recursive: true });
  }

  if (!existsSync(backend.processInfo.logPath)) {
    await appendFile(backend.processInfo.logPath, '', 'utf8');
  }
}

async function startBackendProcess(backend, trigger = 'manual') {
  if (isBackendProcessActive(backend)) {
    return getBackendProcessSnapshot(backend);
  }

  await ensureBackendLogFile(backend);
  backend.processInfo.autoStartDisabled = false;
  backend.processInfo.status = 'starting';
  backend.processInfo.lastStartAttemptAt = Date.now();
  backend.processInfo.lastExitCode = null;
  backend.processInfo.lastExitSignal = null;

  const stdout = createWriteStream(backend.processInfo.logPath, { flags: 'a' });
  const stderr = createWriteStream(backend.processInfo.logPath, { flags: 'a' });
  stdout.write(`\n=== ${new Date().toISOString()} START ${backend.id} trigger=${trigger} ===\n`);

  const child = spawn('/bin/bash', [backend.scriptPath], {
    cwd: MODELS_DIR,
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  });

  backend.processInfo.pid = child.pid;
  backend.processInfo.startedAt = Date.now();

  child.stdout.pipe(stdout);
  child.stderr.pipe(stderr);

  child.on('error', err => {
    backend.processInfo.status = 'error';
    backend.processInfo.lastExitSignal = null;
    backend.processInfo.lastExitCode = null;
    appendFile(backend.processInfo.logPath, `\n[manager:error] ${err.message}\n`, 'utf8').catch(() => {});
  });

  child.on('exit', (code, signal) => {
    backend.processInfo.pid = null;
    backend.processInfo.lastExitCode = code;
    backend.processInfo.lastExitSignal = signal;
    backend.processInfo.status = code === 0 || signal === 'SIGTERM' ? 'stopped' : 'error';
    resetBackendRuntimeState(backend);
    appendFile(
      backend.processInfo.logPath,
      `\n=== ${new Date().toISOString()} EXIT ${backend.id} code=${code} signal=${signal || 'none'} ===\n`,
      'utf8'
    ).catch(() => {});
  });

  child.unref();
  log('PROC', `Starting backend=${backend.id} pid=${child.pid} trigger=${trigger}`);
  return getBackendProcessSnapshot(backend);
}

async function stopBackendProcess(backend, trigger = 'manual') {
  if (!backend.processInfo.pid) {
    backend.processInfo.status = 'stopped';
    if (trigger === 'dashboard' || trigger === 'manual') {
      backend.processInfo.autoStartDisabled = true;
    }
    return getBackendProcessSnapshot(backend);
  }

  backend.processInfo.status = 'stopping';
  if (trigger === 'dashboard' || trigger === 'manual') {
    backend.processInfo.autoStartDisabled = true;
  }
  try {
    process.kill(-backend.processInfo.pid, 'SIGTERM');
    log('PROC', `Stopping backend=${backend.id} pid=${backend.processInfo.pid} trigger=${trigger}`);
  } catch (err) {
    if (err.code === 'ESRCH') {
      backend.processInfo.pid = null;
      backend.processInfo.status = 'stopped';
    } else {
      throw err;
    }
  }

  return getBackendProcessSnapshot(backend);
}

async function restartBackendProcess(backend, trigger = 'manual') {
  await stopBackendProcess(backend, trigger);
  return startBackendProcess(backend, trigger);
}

async function ensureBackendProcess(backend, trigger = 'auto') {
  if (!BACKEND_AUTO_START || backend.processInfo.autoStartDisabled || isBackendProcessActive(backend)) {
    return;
  }

  const lastAttemptAge = Date.now() - backend.processInfo.lastStartAttemptAt;
  if (lastAttemptAge >= 0 && lastAttemptAge < BACKEND_STARTUP_GRACE_MS) {
    return;
  }

  await startBackendProcess(backend, trigger);
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function bootstrapBackendProcesses() {
  await Promise.all(backends.map(backend => ensureBackendProcess(backend, 'bootstrap')));
  await wait(Math.min(BACKEND_STARTUP_GRACE_MS, 4_000));
  await checkAllBackendsHealth();
}

function getHealthyBackends() {
  return backends.filter(backend => backend.healthy);
}

function getTotalQueueLength() {
  return backends.reduce((sum, backend) => sum + backend.slotQueue.length, 0);
}

function pickAvailableSlot(backend, preferredSlotId = null) {
  if (preferredSlotId !== null) {
    const preferredIndex = backend.availableSlots.indexOf(preferredSlotId);
    if (preferredIndex !== -1) {
      return backend.availableSlots.splice(preferredIndex, 1)[0];
    }
  }

  return backend.availableSlots.shift();
}

function acquireSlotOnBackend(backend, reqId, signal, preferredSlotId = null) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      const err = new Error('aborted');
      err.name = 'AbortError';
      return reject(err);
    }

    if (backend.availableSlots.length > 0) {
      const slotId = pickAvailableSlot(backend, preferredSlotId);
      backend.activeSlots.set(reqId, slotId);
      log('SLOT', `${C.green}ACQUIRED${C.reset}  backend=${backend.id} slot=${slotId}  ${reqId}`);
      resolve(slotId);
    } else {
      log('SLOT', `${C.yellow}QUEUED${C.reset}  backend=${backend.id} queue=${backend.slotQueue.length + 1}  ${reqId}`);
      backend.slotQueue.push({ resolve, reject, reqId, signal, preferredSlotId });
    }
  });
}

function releaseSlotOnBackend(backend, reqId) {
  const queueIndex = backend.slotQueue.findIndex(item => item.reqId === reqId);
  if (queueIndex !== -1) {
    const [removed] = backend.slotQueue.splice(queueIndex, 1);
    const err = new Error('aborted');
    err.name = 'AbortError';
    removed.reject(err);
    log('SLOT', `${C.yellow}REMOVED FROM QUEUE${C.reset}  backend=${backend.id}  ${reqId}`);
    return;
  }

  const slotId = backend.activeSlots.get(reqId);
  if (slotId !== undefined && slotId !== null) {
    backend.activeSlots.delete(reqId);

    while (backend.slotQueue.length > 0) {
      let nextIndex = backend.slotQueue.findIndex(item => {
        if (item.signal?.aborted) return false;
        return item.preferredSlotId === null || item.preferredSlotId === slotId;
      });

      if (nextIndex === -1) {
        nextIndex = backend.slotQueue.findIndex(item => !item.signal?.aborted);
      }

      if (nextIndex === -1) {
        break;
      }

      const [next] = backend.slotQueue.splice(nextIndex, 1);
      if (!next.signal?.aborted) {
        backend.activeSlots.set(next.reqId, slotId);
        log('SLOT', `${C.green}PASSED${C.reset}  backend=${backend.id} slot=${slotId}  ${reqId} -> ${next.reqId}`);
        next.resolve(slotId);
        return;
      }

        const err = new Error('aborted');
        err.name = 'AbortError';
        next.reject(err);
    }

    backend.availableSlots.push(slotId);
    log('SLOT', `${C.green}RELEASED${C.reset}  backend=${backend.id} slot=${slotId}  ${reqId}`);
  }
}

function invalidateAffinityForBackend(backendId) {
  for (const map of [affinityIndex.exact, affinityIndex.system, affinityIndex.prefix]) {
    for (const [key, value] of map.entries()) {
      if (value.backendId === backendId) {
        map.delete(key);
      }
    }
  }
}

async function timedFetch(url, timeoutMs, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function checkBackendHealth(backend) {
  const probePaths = ['/health', '/metrics', '/'];
  const wasHealthy = backend.healthy;
  let lastError = 'unreachable';

  for (const path of probePaths) {
    try {
      const res = await timedFetch(`${backend.baseUrl}${path}`, BACKEND_HEALTHCHECK_TIMEOUT_MS);
      if (res.ok || (path === '/' && res.status < 500)) {
        backend.healthy = true;
        if (backend.processInfo.status === 'starting' || backend.processInfo.status === 'stopped' || backend.processInfo.status === 'error') {
          backend.processInfo.status = 'running';
        }
        backend.lastError = null;
        backend.lastCheckAt = Date.now();
        if (!wasHealthy) {
          log('HEALTH', `${C.green}UP${C.reset}  backend=${backend.id}  ${backend.baseUrl}`);
        }
        return true;
      }
      lastError = `status=${res.status}`;
    } catch (err) {
      lastError = err.name === 'AbortError' ? 'timeout' : err.message;
    }
  }

  backend.healthy = false;
  backend.lastError = lastError;
  backend.lastCheckAt = Date.now();
  if (wasHealthy) {
    logWarn(`Backend down: ${backend.id} (${backend.baseUrl}) - ${lastError}`);
  }
  invalidateAffinityForBackend(backend.id);
  await ensureBackendProcess(backend, 'healthcheck');
  return false;
}

async function checkAllBackendsHealth() {
  await Promise.all(backends.map(checkBackendHealth));
}

function chooseRoundRobin(backendsPool) {
  if (backendsPool.length === 0) return null;
  const backend = backendsPool[roundRobinCursor % backendsPool.length];
  roundRobinCursor = (roundRobinCursor + 1) % Number.MAX_SAFE_INTEGER;
  return backend;
}

function hasFreeSlot(backend, preferredSlotId = null) {
  if (preferredSlotId !== null && backend.availableSlots.includes(preferredSlotId)) {
    return true;
  }
  return backend.availableSlots.length > 0;
}

function normalizeText(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function takeTokenPrefix(text, maxTokens = 128) {
  const tokens = normalizeText(text).split(' ').filter(Boolean);
  return tokens.slice(0, maxTokens).join(' ');
}

function prefixSimilarityScore(left, right) {
  const leftTokens = takeTokenPrefix(left, 256).split(' ').filter(Boolean);
  const rightTokens = takeTokenPrefix(right, 256).split(' ').filter(Boolean);
  if (leftTokens.length === 0 || rightTokens.length === 0) {
    return 0;
  }

  const maxLength = Math.max(leftTokens.length, rightTokens.length);
  let matched = 0;
  const limit = Math.min(leftTokens.length, rightTokens.length);
  while (matched < limit && leftTokens[matched] === rightTokens[matched]) {
    matched++;
  }

  return matched / maxLength;
}

function getLoadedSlotMatch(backend, requestContext) {
  let bestMatch = null;
  for (const [slotId, slotContext] of backend.slotContextIndex.entries()) {
    if (!backend.availableSlots.includes(slotId)) continue;

    if (requestContext.exactHash && slotContext.exactHash === requestContext.exactHash) {
      return { slotId, reason: 'exact-hot', score: 1 };
    }

    if (!bestMatch && requestContext.systemHash && slotContext.systemHash === requestContext.systemHash) {
      bestMatch = { slotId, reason: 'system-hot', score: 0.95 };
      continue;
    }

    if (requestContext.normalizedPrefix && slotContext.prefixText) {
      const score = prefixSimilarityScore(requestContext.normalizedPrefix, slotContext.prefixText);
      if (score >= 0.8 && (!bestMatch || score > bestMatch.score)) {
        bestMatch = { slotId, reason: 'prefix-hot', score };
      }
    }
  }

  return bestMatch;
}

function pickQueueBackend(healthyBackends, preferredBackend = null) {
  if (preferredBackend && healthyBackends.includes(preferredBackend)) {
    return preferredBackend;
  }

  const sorted = [...healthyBackends].sort((left, right) => left.slotQueue.length - right.slotQueue.length);
  const shortestLength = sorted[0]?.slotQueue.length;
  const shortest = sorted.filter(backend => backend.slotQueue.length === shortestLength);
  return chooseRoundRobin(shortest);
}

function findAffinityPreference(requestContext) {
  const healthyBackends = getHealthyBackends();
  const isHealthy = backendId => healthyBackends.some(backend => backend.id === backendId);

  if (requestContext.exactHash) {
    const exact = affinityIndex.exact.get(requestContext.exactHash);
    if (exact && isHealthy(exact.backendId)) {
      return {
        backend: backendsById.get(exact.backendId),
        preferredSlotId: exact.slotId,
        reason: 'exact-affinity',
      };
    }
  }

  if (requestContext.systemHash) {
    const system = affinityIndex.system.get(requestContext.systemHash);
    if (system && isHealthy(system.backendId)) {
      return {
        backend: backendsById.get(system.backendId),
        preferredSlotId: system.slotId,
        reason: 'system-affinity',
      };
    }
  }

  let bestPrefix = null;
  if (requestContext.normalizedPrefix) {
    for (const candidate of affinityIndex.prefix.values()) {
      if (!isHealthy(candidate.backendId)) continue;
      const score = prefixSimilarityScore(requestContext.normalizedPrefix, candidate.prefixText);
      if (score >= 0.8 && (!bestPrefix || score > bestPrefix.score)) {
        bestPrefix = {
          backend: backendsById.get(candidate.backendId),
          preferredSlotId: candidate.slotId,
          reason: 'prefix-affinity',
          score,
        };
      }
    }
  }

  if (bestPrefix) {
    return bestPrefix;
  }

  return {
    backend: null,
    preferredSlotId: null,
    reason: 'round-robin',
  };
}

function chooseBackendForRequest(requestContext) {
  const healthyBackends = getHealthyBackends();
  if (healthyBackends.length === 0) {
    throw new Error('No healthy llama backends available');
  }

  const preference = findAffinityPreference(requestContext);
  const freeBackends = healthyBackends.filter(backend => hasFreeSlot(backend));

  if (freeBackends.length === 0) {
    return {
      backend: pickQueueBackend(healthyBackends, preference.backend),
      preferredSlotId: preference.preferredSlotId,
      reason: `${preference.reason}-queued`,
      queued: true,
    };
  }

  if (preference.backend && hasFreeSlot(preference.backend, preference.preferredSlotId)) {
    return {
      backend: preference.backend,
      preferredSlotId: preference.preferredSlotId,
      reason: preference.reason,
      queued: false,
    };
  }

  const hotAlternatives = freeBackends
    .map(backend => ({ backend, match: getLoadedSlotMatch(backend, requestContext) }))
    .filter(candidate => candidate.match !== null);

  if (hotAlternatives.length > 0) {
    hotAlternatives.sort((left, right) => right.match.score - left.match.score);
    return {
      backend: hotAlternatives[0].backend,
      preferredSlotId: hotAlternatives[0].match.slotId,
      reason: `${preference.reason}-spillover-hot`,
      queued: false,
    };
  }

  if (preference.backend) {
    const spilloverPool = freeBackends.filter(backend => backend.id !== preference.backend.id);
    const target = chooseRoundRobin(spilloverPool.length > 0 ? spilloverPool : freeBackends);
    return {
      backend: target,
      preferredSlotId: null,
      reason: `${preference.reason}-spillover-restore`,
      queued: false,
    };
  }

  return {
    backend: chooseRoundRobin(freeBackends),
    preferredSlotId: null,
    reason: 'round-robin',
    queued: false,
  };
}

function upsertAffinity(requestContext, backendId, slotId) {
  const entry = {
    backendId,
    slotId,
    model: requestContext.model,
    prefixText: requestContext.normalizedPrefix,
    updatedAt: Date.now(),
  };

  if (requestContext.exactHash) {
    affinityIndex.exact.set(requestContext.exactHash, entry);
  }
  if (requestContext.systemHash) {
    affinityIndex.system.set(requestContext.systemHash, entry);
  }
  if (requestContext.prefixSignature) {
    affinityIndex.prefix.set(requestContext.prefixSignature, entry);
  }
}

function rememberSlotContext(backend, slotId, requestContext, source) {
  if (slotId === null || slotId === undefined) return;

  backend.slotContextIndex.set(slotId, {
    exactHash: requestContext.exactHash,
    systemHash: requestContext.systemHash,
    prefixSignature: requestContext.prefixSignature,
    prefixText: requestContext.normalizedPrefix,
    source,
    updatedAt: Date.now(),
  });
  upsertAffinity(requestContext, backend.id, slotId);
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

const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailers',
  'transfer-encoding',
  'upgrade',
  'content-length',
]);

function sha256(input) {
  return createHash('sha256').update(input, 'utf8').digest('hex');
}

async function llamaPost(backend, path, payload) {
  const res = await fetch(`${backend.baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '(no body)');
    throw new Error(`llama.cpp ${backend.id}${path} failed ${res.status}: ${text.slice(0, 300)}`);
  }

  return res;
}

function buildProxyRequestHeaders(sourceHeaders, backend) {
  const headers = {};
  for (const [key, value] of Object.entries(sourceHeaders || {})) {
    const lower = key.toLowerCase();
    if (HOP_BY_HOP_HEADERS.has(lower)) continue;
    if (value === undefined) continue;
    headers[key] = Array.isArray(value) ? value.join(', ') : String(value);
  }

  headers.Host = `${backend.host}:${backend.port}`;
  headers['Content-Type'] = headers['Content-Type'] || headers['content-type'] || 'application/json';
  return headers;
}

function buildProxyResponseHeaders(headerEntries, extraHeaders = {}) {
  const headers = {};
  for (const [key, value] of headerEntries.entries()) {
    if (HOP_BY_HOP_HEADERS.has(key.toLowerCase())) continue;
    headers[key] = value;
  }

  return {
    ...headers,
    ...extraHeaders,
  };
}

async function fetchChatCompletion(backend, requestHeaders, payload, signal) {
  return fetch(`${backend.baseUrl}/v1/chat/completions`, {
    method: 'POST',
    headers: buildProxyRequestHeaders(requestHeaders, backend),
    body: JSON.stringify(payload),
    signal,
  });
}

/**
 * Save KV cache — llama.cpp saves to its internal cache dir.
 * We copy the result to L1_DIR (RAMDisk), then persist to L2_DIR (SSD) in background.
 *
 * @returns {{ filename: string, cacheDir: string }}
 *   cacheDir = where llama.cpp saved the file (its internal cache dir)
 */
async function saveKVCache(hash, model, backend, slotId) {
  const filename = `${hash}.bin`;

  try {
    log('SAVE', `Starting  →  ${filename}  backend=${backend.id}`);

    const res = await llamaPost(backend, `/slots/${slotId}?action=save`, { filename, model });
    await res.text();

    log('SAVE', `${C.green}Saved${C.reset}  →  L1 RAMDisk (backend=${backend.id} slot=${slotId})`);
    return { filename, backendId: backend.id };
  } catch (err) {
    logErr(`Save failed for ${hash}.bin (backend=${backend.id} slot=${slotId}): ${err.message}`);
    return null;
  }
}

/**
 * Restore KV cache — copy from L1_DIR to llama.cpp's cache dir,
 * call the restore endpoint, then clean up the temp copy.
 *
 * @param {string} hash - Cache hash
 * @param {string} model - Model name
 */
async function restoreKVCache(hash, model, backend, slotId) {
  const filename = `${hash}.bin`;

  log('RESTORE', `Loading  →  ${filename}  backend=${backend.id} slot=${slotId}  (from L1 RAMDisk)`);

  try {
    const res = await llamaPost(backend, `/slots/${slotId}?action=restore`, { filename, model });
    log('RESTORE', `${C.green}Success${C.reset}  →  ${filename}  (backend=${backend.id} slot=${slotId}, ${res.status})`);
    return true;
  } catch (err) {
    logErr(`Restore failed for ${hash}.bin (backend=${backend.id} slot=${slotId}): ${err.message}`);
    throw err;
  }
}

// ─── 5. SSE Streaming Proxy ────────────────────────────────────────

function createSlotTracker(onSlot) {
  let buffer = '';

  return chunk => {
    buffer += Buffer.from(chunk).toString('utf8');
    if (buffer.length > 65536) {
      buffer = buffer.slice(-32768);
    }

    while (true) {
      const boundary = buffer.indexOf('\n\n');
      if (boundary === -1) break;

      const rawEvent = buffer.slice(0, boundary).replace(/\r/g, '');
      buffer = buffer.slice(boundary + 2);

      for (const line of rawEvent.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const payload = line.slice(5).trim();
        if (!payload || payload === '[DONE]') continue;

        try {
          const parsed = JSON.parse(payload);
          const slotId = parsed.slot_id ?? parsed.id_slot;
          if (Number.isInteger(slotId)) {
            onSlot(slotId);
          }
        } catch {
          // Ignore partial or non-JSON SSE frames.
        }
      }
    }
  };
}

function normalizeSlotId(slotId, fallbackSlotId, backend) {
  if (Number.isInteger(slotId) && slotId >= 0 && slotId < backend.maxSlots) {
    return slotId;
  }
  return fallbackSlotId;
}

async function proxyStreamWithHeaders(llamaRes, reply, extraHeaders, signal, onChunk) {
  reply.hijack();

  reply.raw.writeHead(llamaRes.status, extraHeaders);

  const reader = llamaRes.body?.getReader();
  if (reader) {
    try {
      while (!signal.aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        if (signal.aborted) break;
        if (onChunk) onChunk(value);
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

function extractRequestContext(body) {
  const messages = Array.isArray(body?.messages) ? body.messages : [];
  const promptText = typeof body?.prompt === 'string'
    ? body.prompt
    : (body?.prompt ? JSON.stringify(body.prompt) : '');
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

  const model = body?.model || null;
  const normalizedSystemPrompt = normalizeText(systemPrompt);
  const affinitySource = promptText || [systemPrompt, firstUserMessage?.content || ''].filter(Boolean).join('\n\n');
  const normalizedPrefix = takeTokenPrefix(affinitySource, 256);
  const modelPrefix = model ? `${model}_` : '';
  const exactHash = normalizedPrefix ? sha256(`${modelPrefix}${normalizedPrefix}`) : null;
  const systemHash = normalizedSystemPrompt ? sha256(`${modelPrefix}${normalizedSystemPrompt}`) : null;
  const prefixSignature = normalizedPrefix ? sha256(`${modelPrefix}${takeTokenPrefix(normalizedPrefix, 128)}`) : null;

  return {
    model,
    promptText,
    systemPrompt,
    normalizedSystemPrompt,
    firstUserMessage,
    userMessages,
    normalizedPrefix,
    exactHash,
    systemHash,
    prefixSignature,
    hasAffinity: Boolean(normalizedPrefix || normalizedSystemPrompt),
    canUseCache: Boolean(systemPrompt && exactHash),
  };
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

function parsePrometheusMetrics(text) {
  const metrics = {};
  text.split('\n').forEach(line => {
    if (!line || line.startsWith('#') || !line.includes(' ')) return;
    const [key, rawValue] = line.split(' ');
    const numericValue = parseFloat(rawValue);
    if (!Number.isNaN(numericValue)) {
      metrics[key] = numericValue;
    }
  });
  return metrics;
}

async function fetchBackendMetrics(backend) {
  try {
    const response = await timedFetch(`${backend.baseUrl}/metrics`, BACKEND_HEALTHCHECK_TIMEOUT_MS);
    if (!response.ok) return {};
    return parsePrometheusMetrics(await response.text());
  } catch {
    return {};
  }
}

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

  const backendSnapshots = await Promise.all(backends.map(async backend => {
    const metrics = await fetchBackendMetrics(backend);
    return { backend, metrics };
  }));

  const llamaMetrics = {};
  for (const snapshot of backendSnapshots) {
    for (const [key, value] of Object.entries(snapshot.metrics)) {
      llamaMetrics[key] = (llamaMetrics[key] || 0) + value;
    }
  }

  const slotsInfo = [];
  for (const backend of backends) {
    for (let slotId = 0; slotId < backend.maxSlots; slotId++) {
      let activeReq = null;
      for (const [requestId, activeSlotId] of backend.activeSlots.entries()) {
        if (activeSlotId === slotId) {
          activeReq = requestId;
          break;
        }
      }

      const slotContext = backend.slotContextIndex.get(slotId);
      slotsInfo.push({
        id: `${backend.id}:${slotId}`,
        backendId: backend.id,
        backendLabel: `${backend.host}:${backend.port}`,
        hash: slotContext?.exactHash || null,
        activeReq,
      });
    }
  }

  reply.send({
    queueLength: getTotalQueueLength(),
    l1UsageBytes,
    slots: slotsInfo,
    llamaMetrics,
    backends: backendSnapshots.map(({ backend, metrics }) => ({
      id: backend.id,
      url: backend.baseUrl,
      gpuGroup: backend.gpuGroup,
      healthy: backend.healthy,
      queueLength: backend.slotQueue.length,
      activeRequests: backend.activeSlots.size,
      availableSlots: backend.availableSlots.length,
      lastCheckAt: backend.lastCheckAt,
      lastError: backend.lastError,
      process: getBackendProcessSnapshot(backend),
      metrics,
    })),
  });
});

server.get('/api/backends/:backendId/logs', async (request, reply) => {
  const backend = backendsById.get(request.params.backendId);
  if (!backend) {
    reply.status(404).send({ error: 'backend_not_found' });
    return;
  }

  const requestedLines = Number.parseInt(request.query?.lines, 10);
  const lineCount = Number.isFinite(requestedLines) ? Math.min(Math.max(requestedLines, 20), 500) : 120;
  const content = await tailFile(backend.processInfo.logPath, lineCount);
  reply.send({
    backendId: backend.id,
    logPath: backend.processInfo.logPath,
    lines: lineCount,
    content,
  });
});

server.post('/api/backends/:backendId/:action', async (request, reply) => {
  const backend = backendsById.get(request.params.backendId);
  if (!backend) {
    reply.status(404).send({ error: 'backend_not_found' });
    return;
  }

  const { action } = request.params;
  if (!['start', 'stop', 'restart'].includes(action)) {
    reply.status(400).send({ error: 'invalid_action' });
    return;
  }

  if (action === 'start') {
    await startBackendProcess(backend, 'dashboard');
  } else if (action === 'stop') {
    await stopBackendProcess(backend, 'dashboard');
  } else {
    await restartBackendProcess(backend, 'dashboard');
  }

  await checkBackendHealth(backend);
  reply.send({
    backendId: backend.id,
    action,
    healthy: backend.healthy,
    process: getBackendProcessSnapshot(backend),
  });
});

server.post('/v1/chat/completions', async (request, reply) => {
  const body = request.body || {};
  const reqId = (body?.metadata && body.metadata?.request_id)
    || body?.id
    || request.id;

  const requestContext = extractRequestContext(body);
  logRequestToFile(reqId, requestContext.systemPrompt || requestContext.promptText, requestContext.firstUserMessage);

  // ─── AbortController + disconnect handler (Fix #1: Zombie Slots) ─
  const abortController = new AbortController();
  const { signal } = abortController;

  let slotReleased = false;
  let selectedBackend = null;
  let assignedSlotId = null;
  let observedSlotId = null;
  let resolveInFlight = null;
  let cacheMarkedActive = false;

  const safeReleaseSlot = () => {
    if (!slotReleased && selectedBackend && assignedSlotId !== null) {
      slotReleased = true;
      releaseSlotOnBackend(selectedBackend, reqId);
    }

    if (resolveInFlight) {
      const releaseInFlight = resolveInFlight;
      resolveInFlight = null;
      releaseInFlight();
    }
  };

  const markCacheBusy = () => {
    if (!cacheMarkedActive && requestContext.exactHash) {
      markCacheActive(requestContext.exactHash);
      cacheMarkedActive = true;
    }
  };

  request.raw.on('aborted', () => {
    if (!reply.raw.writableEnded) {
      log('DISCONNECT', `Client dropped  ${reqId}`);
      abortController.abort();
      safeReleaseSlot();
    }
  });

  const filename = requestContext.exactHash ? `${requestContext.exactHash}.bin` : null;
  const l1Path = filename ? join(L1_DIR, filename) : null;
  const l2Path = filename ? join(L2_DIR, filename) : null;

  if (requestContext.canUseCache && requestContext.exactHash && inFlightMisses.has(requestContext.exactHash)) {
    log('DEDUP', `Waiting for in-flight cache creation for ${requestContext.exactHash.slice(0, 8)}`);
    try {
      await new Promise((resolve, reject) => {
        const onAbort = () => {
          const err = new Error('aborted');
          err.name = 'AbortError';
          reject(err);
        };
        if (signal.aborted) return onAbort();
        signal.addEventListener('abort', onAbort);

        inFlightMisses.get(requestContext.exactHash).push(() => {
          signal.removeEventListener('abort', onAbort);
          resolve();
        });
      });
    } catch (err) {
      if (err.name === 'AbortError') return;
      throw err;
    }
  }

  const l1Exists = Boolean(filename && existsSync(l1Path));
  const l2Exists = Boolean(filename && existsSync(l2Path));
  const routing = chooseBackendForRequest(requestContext);
  selectedBackend = routing.backend;

  log('ROUTE', `backend=${selectedBackend.id} reason=${routing.reason} req=${reqId}`);

  try {
    assignedSlotId = await acquireSlotOnBackend(selectedBackend, reqId, signal, routing.preferredSlotId);
    observedSlotId = assignedSlotId;

    let proxyBody = { ...body, id_slot: assignedSlotId };
    let cacheStatus = 'BYPASS';
    let shouldSaveCache = false;
    const trackedSlot = selectedBackend.slotContextIndex.get(assignedSlotId);

    if (requestContext.canUseCache && (l1Exists || l2Exists)) {
      markCacheBusy();
      let cacheReady = trackedSlot?.exactHash === requestContext.exactHash;

      if (!cacheReady) {
        try {
          if (!l1Exists && l2Exists) {
            logColdHit(requestContext.exactHash);
            await copyFileL2ToL1(requestContext.exactHash);
          } else {
            logHit(requestContext.exactHash);
          }

          await restoreKVCache(requestContext.exactHash, requestContext.model, selectedBackend, assignedSlotId);
          cacheReady = true;
        } catch (err) {
          log('WARN', `Restore failed (${err.message}) — falling back to full request`);
        }
      }

      if (cacheReady) {
        proxyBody = { ...body, messages: requestContext.userMessages, id_slot: assignedSlotId };
        cacheStatus = 'HIT';
      }
    } else if (requestContext.canUseCache && !l1Exists && !l2Exists) {
      markCacheBusy();
      logMiss(requestContext.exactHash);
      inFlightMisses.set(requestContext.exactHash, []);
      resolveInFlight = () => {
        const waiters = inFlightMisses.get(requestContext.exactHash) || [];
        inFlightMisses.delete(requestContext.exactHash);
        waiters.forEach(waiter => waiter());
      };
      cacheStatus = 'MISS';
      shouldSaveCache = true;
    }

    const observeSlot = createSlotTracker(slotId => {
      observedSlotId = slotId;
    });

    const llamaRes = await fetchChatCompletion(selectedBackend, request.headers, proxyBody, signal);
    if (!llamaRes.ok) {
      const errText = await llamaRes.text().catch(() => '');
      logErr(`llama.cpp ${selectedBackend.id} returned ${llamaRes.status}: ${errText.slice(0, 500)}`);
      throw new Error(`llama.cpp ${selectedBackend.id} returned ${llamaRes.status}: ${errText.slice(0, 300)}`);
    }

    const responseHeaders = buildProxyResponseHeaders(llamaRes.headers, {
      'X-Accel-Buffering': 'no',
      'X-Backend-Id': selectedBackend.id,
      'X-Backend-Url': selectedBackend.baseUrl,
    });
    if (cacheStatus !== 'BYPASS') {
      responseHeaders['X-Cache-Status'] = cacheStatus;
    }

    await proxyStreamWithHeaders(llamaRes, reply, responseHeaders, signal, observeSlot);

    const effectiveSlotId = normalizeSlotId(observedSlotId, assignedSlotId, selectedBackend);
    if (requestContext.hasAffinity) {
      rememberSlotContext(selectedBackend, effectiveSlotId, requestContext, cacheStatus === 'HIT' ? 'hit' : 'stream');
    }

    if (shouldSaveCache && !signal.aborted) {
      try {
        const saveResult = await saveKVCache(requestContext.exactHash, requestContext.model, selectedBackend, assignedSlotId);
        if (saveResult) {
          rememberSlotContext(selectedBackend, effectiveSlotId, requestContext, 'save');
          copyFile(join(L1_DIR, `${requestContext.exactHash}.bin`), l2Path)
            .then(() => {
              log('SAVE', `${C.green}Persisted${C.reset}  →  L2 SSD`);
            })
            .catch(err => logWarn(`L2 persist failed: ${err.message}`));
        }
      } catch (err) {
        logWarn(`Save pipeline failed: ${err.message}`);
      }
    }
  } finally {
    if (cacheMarkedActive && requestContext.exactHash) {
      markCacheInactive(requestContext.exactHash);
    }
    safeReleaseSlot();
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

  // Prime backend health state before accepting traffic.
  await checkAllBackendsHealth();
  await bootstrapBackendProcesses();

  // Start GC sweeper (L1 eviction + age-based cleanup)
  const gcInterval = setInterval(async () => {
    await sweepL1Eviction();
    await gcSweep();
  }, GARBAGE_COLLECT_INTERVAL_MS);
  if (gcInterval.unref) gcInterval.unref();

  const healthInterval = setInterval(() => {
    checkAllBackendsHealth().catch(err => {
      logWarn(`Health sweep failed: ${err.message}`);
    });
  }, BACKEND_HEALTHCHECK_INTERVAL_MS);
  if (healthInterval.unref) healthInterval.unref();

  try {
    await server.listen({ port: 8080, host: '0.0.0.0' });

    const PORT = 8080;
    const stripAnsi = (str) => str.replace(/\x1b\[[0-9;]*m/g, '');
    const backendSummary = backends
      .map(backend => `${backend.id}=${backend.baseUrl} gpu:${backend.gpuGroup} slots:${backend.maxSlots}`)
      .join(' | ');
    const lines = [
      `  ${C.bold}${C.green}KV Cache Semantic Router — Tiered Storage${C.reset}`,
      `  Listening on  ${C.bold}http://0.0.0.0:${PORT}${C.reset}`,
      `  Dashboard UI  ${C.bold}${C.magenta}http://127.0.0.1:${PORT}/dashboard${C.reset}`,
      `  Backends      ${C.bold}${backendSummary}${C.reset}`,
      `  L1 (RAMDisk)  ${C.bold}${L1_DIR}${C.reset} (tmpfs)`,
      `  L2 (SSD)      ${C.bold}${L2_DIR}${C.reset} (persistent)`,
      `  Health check  ${C.bold}${BACKEND_HEALTHCHECK_INTERVAL_MS / 1000}s${C.reset} timeout ${C.bold}${BACKEND_HEALTHCHECK_TIMEOUT_MS}ms${C.reset}`,
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
