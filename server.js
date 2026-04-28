#!/usr/bin/env node
/**
 * KV Cache Semantic Router
 * ========================
 * Inverse API Gateway for llama.cpp — caches KV state by hashing the
 * System Prompt and saving / restoring per-slot .bin files on disk.
 *
 * Usage:
 *   node server.js
 *
 * Listens on :8080, proxies to llama.cpp at :11434.
 */

'use strict';

// ─── Dependencies ───────────────────────────────────────────────────
// npm install fastify @fastify/cors @fastify/formbody
// (fetch is native in Node ≥ 18)

const { createHash } = require('crypto');
const { existsSync, mkdirSync } = require('fs');
const { join } = require('path');
const fastify = require('fastify');
const fastifyCors = require('@fastify/cors');
const fastifyFormBody = require('@fastify/formbody');

// ─── Configuration ──────────────────────────────────────────────────
const PORT = 8080;
const LLAMA_HOST = '127.0.0.1';
const LLAMA_PORT = 11434;
const LLAMA_BASE = `http://${LLAMA_HOST}:${LLAMA_PORT}`;
const CACHE_DIR = join('/home/daniel', 'Models', 'slot_cache');

// ─── Helpers ────────────────────────────────────────────────────────

// ANSI colour helpers for terminal output
const C = {
  reset:  '\x1b[0m',
  bold:   '\x1b[1m',
  grey:   '\x1b[90m',
  green:  '\x1b[32m',
  yellow: '\x1b[33m',
  red:    '\x1b[31m',
  cyan:   '\x1b[36m',
  blue:   '\x1b[34m',
};

function log(tag, msg) {
  console.log(`${C.grey}${new Date().toISOString().slice(11, 23)}${C.reset}  ${C.bold}[${tag}]${C.reset} ${msg}`);
}

function logHit(hash) {
  console.log(
    `${C.green}${C.bold}■ CACHE HIT${C.reset}  slot  →  ${hash}.bin  (restoring from disk)`
  );
}

function logMiss(hash) {
  console.log(
    `${C.yellow}${C.bold}■ CACHE MISS${C.reset}  slot  →  ${hash}.bin  (will save after response)`
  );
}

function logErr(msg) {
  console.error(`${C.red}${C.bold}✖ ERROR${C.reset}   ${msg}`);
}

// ─── Ensure cache directory exists ──────────────────────────────────
if (!existsSync(CACHE_DIR)) {
  mkdirSync(CACHE_DIR, { recursive: true });
  log('INIT', `Created cache directory: ${CACHE_DIR}`);
}

// ─── Fastify server ─────────────────────────────────────────────────
const server = fastify({
  bodyLimit: 50 * 1024 * 1024,   // 50 MB
  maxParamLength: 1024 * 1024,
  disableRequestLogging: true,
});

server.register(fastifyCors, { origin: true });
server.register(fastifyFormBody);

// ─── Core logic ─────────────────────────────────────────────────────

/**
 * Extract the System Prompt and User Messages from an OpenAI-format
 * /v1/chat/completions request body.
 */
function extractSystemPrompt(body) {
  const messages = Array.isArray(body?.messages) ? body.messages : [];
  let systemPrompt = '';
  const userMessages = [];

  let systemFound = false;
  for (const msg of messages) {
    if (!systemFound && msg.role === 'system' && msg.content) {
      systemPrompt = typeof msg.content === 'string'
        ? msg.content
        : JSON.stringify(msg.content);
      systemFound = true;
    } else {
      userMessages.push(msg);
    }
  }

  return { systemPrompt, userMessages };
}

function sha256(str) {
  return createHash('sha256').update(str, 'utf8').digest('hex');
}

/**
 * POST helper using native fetch (Node ≥ 18).
 */
async function llamaPost(path, payload) {
  const res = await fetch(`${LLAMA_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '(no body)');
    throw new Error(
      `llama.cpp ${path} failed ${res.status}: ${text.slice(0, 300)}`
    );
  }

  return res;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Save the KV cache for slot 0 to disk (background, fire-and-forget).
 */
async function saveKVCache(hash, model) {
  const filename = `${hash}.bin`;

  try {
    log('SAVE', `Starting  →  ${filename}`);

    const res = await llamaPost('/slots/0?action=save', { filename, model });

    const bodyText = await res.text().catch(() => '');
    let savedFile = filename;
    try {
      const parsed = JSON.parse(bodyText);
      if (parsed?.path) savedFile = parsed.path;
    } catch { /* ignore */ }

    log('SAVE', `${C.green}Success${C.reset}  →  ${savedFile}  (${res.status})`);
  } catch (err) {
    logErr(`Save failed for ${hash}.bin: ${err.message}`);
  }
}

/**
 * Restore the KV cache for slot 0 from disk.
 */
async function restoreKVCache(hash, model) {
  const filename = `${hash}.bin`;

  try {
    log('RESTORE', `Loading  →  ${filename}`);

    const res = await llamaPost('/slots/0?action=restore', { filename, model });

    log('RESTORE', `${C.green}Success${C.reset}  →  ${filename}  (${res.status})`);
  } catch (err) {
    logErr(`Restore failed for ${hash}.bin: ${err.message}`);
    throw err;
  }
}

/**
 * SSE streaming proxy via hijack.
 */
async function proxyStream(llamaRes) {
  reply.hijack();

  reply.raw.writeHead(llamaRes.status, {
    'Content-Type':    'text/event-stream',
    'Cache-Control':   'no-cache',
    'Connection':      'keep-alive',
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

// ─── Route handler ──────────────────────────────────────────────────
server.post('/v1/chat/completions', async (request, reply) => {
  const body = request.body;
  const reqId = (body?.metadata && body.metadata?.request_id)
    || body?.id
    || '????';

  // 1. Extract system prompt
  const { systemPrompt, userMessages } = extractSystemPrompt(body);
  const hash = systemPrompt ? sha256(systemPrompt) : null;
  const model = body?.model || null;

   // No system prompt → direct proxy
  if (!hash) {
    log('PROXY', `No system prompt — forwarding directly (id=${reqId})`);

    const llamaRes = await fetch(`${LLAMA_BASE}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Host': `${LLAMA_HOST}:${LLAMA_PORT}`,
      },
      body: JSON.stringify(body),
    });

    await proxyStream(llamaRes);
    return;
  }

  const filename = `${hash}.bin`;
  const cacheHit = existsSync(join(CACHE_DIR, filename));

  // ──────────────────────────────────────────────────────────────
  // CACHE HIT — restore KV cache, then proxy with user messages only
  // ──────────────────────────────────────────────────────────────
  if (cacheHit) {
    logHit(hash);

    try {
      await restoreKVCache(hash, model);
      log('INFO', `KV cache restored for ${hash.slice(0, 12)}...`);
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

    reply.raw.writeHead(llamaRes.status, {
      'Content-Type':    'text/event-stream',
      'Cache-Control':   'no-cache',
      'Connection':      'keep-alive',
      'X-Accel-Buffering': 'no',
      'X-Cache-Status':  'HIT',
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
    return;
  }

  // ──────────────────────────────────────────────────────────────
  // CACHE MISS — proxy full request, save KV cache after stream
  // ──────────────────────────────────────────────────────────────
  logMiss(hash);

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
    'Content-Type':    'text/event-stream',
    'Cache-Control':   'no-cache',
    'Connection':      'keep-alive',
    'X-Accel-Buffering': 'no',
    'X-Cache-Status':  'MISS',
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

    // Save KV cache after stream is fully consumed
    (async () => {
      await pump();
      await saveKVCache(hash, model);
    })();
  } else {
    reply.raw.end();
    setImmediate(() => saveKVCache(hash, model));
  }

  return;
});

// ─── Error handler ──────────────────────────────────────────────────
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

// ─── Startup ────────────────────────────────────────────────────────
const start = async () => {
  try {
    await server.listen({ port: PORT, host: '0.0.0.0' });

    console.log(`
${C.bold}${C.cyan}╔═══════════════════════════════════════════════════════════╗${C.reset}
${C.bold}${C.cyan}║${C.reset}  ${C.bold}${C.green}KV Cache Semantic Router${C.reset}                                ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Listening on  ${C.bold}http://0.0.0.0:${PORT}${C.reset}                      ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Proxy target  ${C.bold}${LLAMA_BASE}${C.reset}                                    ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}║${C.reset}  Cache dir     ${C.bold}${CACHE_DIR}${C.reset}                                    ${C.bold}${C.cyan}║${C.reset}
${C.bold}${C.cyan}╚═══════════════════════════════════════════════════════════╝${C.reset}
`);
  } catch (err) {
    logErr(`Cannot start server: ${err.message}`);
    process.exit(1);
  }
};

start();
