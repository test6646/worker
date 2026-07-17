// Fyers live-price worker: one upstream fyersDataSocket shared across all
// browsers, driven by a single state-machine heartbeat. States:
// idle → connecting → live → cooldown → idle. Failure = set cooldownDeadline;
// heartbeat handles the rest. SDK is a hard singleton — built once, listeners
// attached once, epoch-gated. Token rotation → process.exit(0).
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'http';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { createRequire } from 'module';
import { verifyTicket } from './ticket.js';
import { mountSyncRoute } from './sync.js';

dotenv.config();
const require = createRequire(import.meta.url);
let fyersSDK = require('fyers-api-v3');

// Purge fyers-api-v3 (and its HSM/datasocket sub-modules) from require.cache
// and re-require it. The SDK caches its dataSocket instance in module scope,
// so a stale token can otherwise survive `disposeSdkSocket()` and cause
// permanent "Please provide valid token" errors after re-login. Reloading
// the module guarantees a clean slate.
function reloadFyersSDK() {
  try {
    const rootId = require.resolve('fyers-api-v3');
    for (const id of Object.keys(require.cache)) {
      if (id === rootId || id.includes(`${require('path').sep}fyers-api-v3${require('path').sep}`)) {
        delete require.cache[id];
      }
    }
    fyersSDK = require('fyers-api-v3');
  } catch (e) {
    console.warn('[upstream] reloadFyersSDK failed:', e?.message);
  }
}

// ─── Config ─────────────────────────────────────────────────────────────
const envInt = (n, d) => { const v = process.env[n]; const x = Number(v); return v != null && v !== '' && Number.isFinite(x) ? x : d; };
const envStr = (n, d) => { const v = process.env[n]; return v != null && v !== '' ? v : d; };

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  FYERS_APP_ID,
  FYERS_TICKET_SECRET,
  ALLOWED_ORIGINS = '',
} = process.env;

const PORT                       = envInt('PORT', 3000);
const MARKET_TZ                  = envStr('MARKET_TZ', 'Asia/Kolkata');
const MARKET_OPEN_MIN            = envInt('MARKET_OPEN_MIN', 555);          // 09:15 IST
const MARKET_CLOSE_MIN           = envInt('MARKET_CLOSE_MIN', 932);         // 15:32 IST (2-min post-close grace)
const HEARTBEAT_MS               = envInt('HEARTBEAT_MS', 1_000);
const SESSION_POLL_MS            = envInt('SESSION_POLL_MS', 60_000);
const WATCHED_REFRESH_MS         = envInt('WATCHED_REFRESH_MS', 30_000);
const MARKET_STATUS_BROADCAST_MS = envInt('MARKET_STATUS_BROADCAST_MS', 30_000);
const LAST_QUOTE_TTL_MS          = envInt('LAST_QUOTE_TTL_MS', 5_000);
const SETTLE_THROTTLE_MS         = envInt('SETTLE_THROTTLE_MS', 400);
const CONNECT_TIMEOUT_MS         = envInt('UPSTREAM_CONNECT_TIMEOUT_MS', 15_000);
const RECONNECT_BASE_MS          = envInt('UPSTREAM_RECONNECT_BASE_MS', 1_000);
const RECONNECT_MAX_MS           = envInt('UPSTREAM_RECONNECT_MAX_MS', 30_000);
const LIVENESS_MS                = envInt('UPSTREAM_LIVENESS_MS', 90_000);
// Chunked subscribe: single large SymbolUpdate.subscribe silently drops heavy indices.
const SUBSCRIBE_CHUNK_SIZE       = envInt('SUBSCRIBE_CHUNK_SIZE', 40);
const SUBSCRIBE_CHUNK_GAP_MS     = envInt('SUBSCRIBE_CHUNK_GAP_MS', 30);
const FYERS_DATA_CHANNEL         = envInt('FYERS_DATA_CHANNEL', 1);
// Backoff only resets after a session stays live this long — prevents the
// "connect → 3s later close → 2s cooldown → repeat" storm that made every
// cycle look like attempt #1.
const STABLE_SESSION_MS          = envInt('UPSTREAM_STABLE_SESSION_MS', 30_000);
// Canary symbol: always subscribed upstream, used as the transport-liveness
// signal instead of aggregate `streaming.size` (which can be silent for
// illiquid F&O without meaning the socket died).
const CANARY_SYMBOL              = envStr('UPSTREAM_CANARY_SYMBOL', 'NSE:NIFTY50-INDEX');
const HOLIDAYS_REFRESH_MS        = envInt('HOLIDAYS_REFRESH_MS', 6 * 60 * 60 * 1000);
const RPC_RETRY_ATTEMPTS         = envInt('RPC_RETRY_ATTEMPTS', 2);
const RPC_RETRY_DELAY_MS         = envInt('RPC_RETRY_DELAY_MS', 200);

for (const [k, v] of Object.entries({
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, FYERS_APP_ID, FYERS_TICKET_SECRET,
})) {
  if (!v) { console.error(`❌ missing env: ${k}`); process.exit(1); }
}

const allowedOrigins = ALLOWED_ORIGINS.split(',').map((s) => s.trim()).filter(Boolean);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ─── Fanout state ───────────────────────────────────────────────────────
const subscribers      = new Set();  // { ws, symbols:Set<string> }
const refCounts        = new Map();  // symbol -> count
const priceMap         = new Map();  // symbol -> latest tick
const prevLtp          = new Map();  // symbol -> number (for dir)
const streaming        = new Set();  // symbols currently subscribed upstream
const watchedSymbols   = new Set();  // settle_pending_orders trigger set
const lastQuoteWriteAt = new Map();  // symbol -> ms of last snapshot write
const lastSnapshotSig  = new Map();  // symbol -> dedup signature

const state = {
  upstream: 'idle', token: null, sock: null, epoch: 0, attempts: 0, lastTickAt: 0,
  sessionDeadline: 0, watchedDeadline: 0, broadcastDeadline: 0,
  connectDeadline: 0, cooldownDeadline: 0,
};
let lastSettleAt = 0, settleInFlight = false, subscribeInFlight = false, sessionInFlight = false;
// Tracks in-flight snapshot/settle RPCs so SIGTERM can drain writes before exit.
let inflightWrites = 0;
// Detects the closed→open transition to wipe intraday accumulators so
// yesterday's high/low can't leak into today's first-tick composition.
let wasMarketOpen = false;

// fyersDataSocket is a hard module-level singleton — one `new` per process,
// but disposed and rebuilt in-place on token rotation so a mid-session
// re-login resumes ticks immediately (no process.exit, no deferral).
let sdkSock = null, sdkListenersAttached = false, sdkTokenInUse = null;
let stableTimer = null;          // fires after STABLE_SESSION_MS of live+ticks → resets attempts

// Rolling diagnostics
const liveSessionDurationsMs = []; // last 10 completed live sessions
let liveEnteredAt = 0;
let consecutiveShortLivedSessions = 0;
let lastCloseReason = null;
let lastCanaryTickAt = 0;
const rpcFailureCounts = { apply_live_tick_snapshot: 0, settle_pending_orders: 0 };

// Monotonic log sequence + hrtime so log-buffer artifacts are distinguishable
// from real double-fires.
let __logSeq = 0;
const __bootHr = process.hrtime.bigint();
function upstreamLog(level, msg) {
  __logSeq += 1;
  const hrMs = Number((process.hrtime.bigint() - __bootHr) / 1_000_000n);
  const line = `[upstream #${__logSeq} +${hrMs}ms] ${msg}`;
  (console[level] ?? console.log)(line);
}

// ─── Market hours: ICU-safe Intl formatter + Supabase holiday calendar ──
const IST_PARTS = new Intl.DateTimeFormat('en-US', {
  timeZone: MARKET_TZ, hour12: false,
  weekday: 'short', hour: 'numeric', minute: 'numeric', second: 'numeric',
  year: 'numeric', month: '2-digit', day: '2-digit',
});
function istNow() {
  const parts = Object.fromEntries(IST_PARTS.formatToParts(new Date()).map((p) => [p.type, p.value]));
  const hour = Number(parts.hour === '24' ? '0' : parts.hour); // Intl 'h23' quirk
  const minute = Number(parts.minute);
  const second = Number(parts.second);
  const weekday = parts.weekday; // 'Mon'..'Sun'
  const isoDate = `${parts.year}-${parts.month}-${parts.day}`;
  return { hour, minute, second, weekday, isoDate };
}

const holidaySet = new Set();
async function refreshHolidays() {
  try {
    const { data, error } = await supabase
      .from('market_holidays')
      .select('holiday_date')
      .eq('exchange', 'NSE');
    if (error) throw error;
    holidaySet.clear();
    for (const row of data ?? []) if (row?.holiday_date) holidaySet.add(String(row.holiday_date));
    console.log(`[holidays] loaded ${holidaySet.size} NSE holidays`);
  } catch (e) {
    console.warn('[holidays]', e?.message ?? e);
  }
}

function setUpstream(next, reason = '') {
  if (state.upstream === next) return;
  upstreamLog('log', `${state.upstream} → ${next}${reason ? ` (${reason})` : ''}`);
  const prev = state.upstream;
  state.upstream = next;
  if (next === 'live') {
    liveEnteredAt = Date.now();
  } else if (prev === 'live' && liveEnteredAt) {
    const dur = Date.now() - liveEnteredAt;
    liveSessionDurationsMs.push(dur);
    while (liveSessionDurationsMs.length > 10) liveSessionDurationsMs.shift();
    if (dur < STABLE_SESSION_MS) consecutiveShortLivedSessions += 1;
    else consecutiveShortLivedSessions = 0;
    liveEnteredAt = 0;
  }
  broadcastStatus(next === 'live' ? 'connected' : next);
}

function isMarketOpen() {
  const { weekday, hour, minute, second, isoDate } = istNow();
  if (weekday === 'Sat' || weekday === 'Sun') return false;
  if (holidaySet.has(isoDate)) return false;
  const s = (hour * 60 + minute) * 60 + second;
  return s >= MARKET_OPEN_MIN * 60 && s < (MARKET_CLOSE_MIN + 1) * 60;
}

function computeBackoffMs() {
  const raw = Math.min(RECONNECT_BASE_MS * Math.pow(2, state.attempts), RECONNECT_MAX_MS);
  return Math.round(raw * (0.8 + Math.random() * 0.4)); // ±20% jitter
}

async function loadAccessToken() {
  const { data, error } = await supabase
    .from('fyers_session')
    .select('access_token, expires_at')
    .eq('id', true)
    .maybeSingle();
  if (error) { console.warn('[session]', error.message); return null; }
  if (!data?.access_token) return null;
  if (new Date(data.expires_at).getTime() < Date.now()) return null;
  return data.access_token;
}

// One-shot REST validation to catch bad tokens before we let them anywhere
// near the WebSocket. Returns true only on Fyers `s: "ok"`; any error or
// non-ok response is treated as "do not connect".
async function validateFyersToken(token) {
  if (!token) return false;
  const bearer = `${FYERS_APP_ID}:${token}`;
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 5000);
    const res = await fetch('https://api-t1.fyers.in/api/v3/profile', {
      headers: { Authorization: bearer, Accept: 'application/json' },
      signal: ctrl.signal,
    }).finally(() => clearTimeout(t));
    if (!res.ok) { console.warn('[session] validate http', res.status); return false; }
    const j = await res.json().catch(() => null);
    if (j?.s === 'ok') return true;
    console.warn('[session] validate reject:', j?.code, j?.message);
    return false;
  } catch (e) {
    console.warn('[session] validate threw:', e?.message);
    return false;
  }
}

// Bounded RPC retry — 2 attempts with short delay so transient Supabase
// blips don't silently drop a tick snapshot or a settle trigger. Wrapped
// with an `inflightWrites` gauge so SIGTERM can drain outstanding writes.
async function rpcWithRetry(name, args) {
  inflightWrites += 1;
  try {
    let lastErr = null;
    for (let attempt = 0; attempt <= RPC_RETRY_ATTEMPTS; attempt += 1) {
      const { error } = await supabase.rpc(name, args);
      if (!error) {
        if (rpcFailureCounts[name] != null) rpcFailureCounts[name] = 0;
        return true;
      }
      lastErr = error;
      if (attempt < RPC_RETRY_ATTEMPTS) await new Promise((r) => setTimeout(r, RPC_RETRY_DELAY_MS));
    }
    if (rpcFailureCounts[name] != null) rpcFailureCounts[name] += 1;
    console.warn(`[rpc:${name}] failed after ${RPC_RETRY_ATTEMPTS + 1} attempts:`, lastErr?.message);
    return false;
  } finally {
    inflightWrites = Math.max(0, inflightWrites - 1);
  }
}

// ─── Upstream lifecycle ─────────────────────────────────────────────────
function acquireSocket(fullToken) {
  const F = fyersSDK.fyersDataSocket;
  if (!F) throw new Error('fyersDataSocket missing from SDK');
  if (!sdkSock) {
    // Prefer fresh construction — the SDK's `getInstance` is a module-level
    // singleton that ignores subsequent tokens and returns the previously
    // cached instance still bound to an expired token. `new F(...)` yields
    // a fresh socket bound to whatever token we pass.
    try {
      sdkSock = new F(fullToken, '', false);
    } catch (e) {
      if (typeof F.getInstance === 'function') sdkSock = F.getInstance(fullToken, '', false);
      else throw e;
    }
    sdkTokenInUse = fullToken;
  }
  return sdkSock;
}

// Dispose the SDK singleton so a new one can be constructed against a fresh
// token. Called on token rotation. Any late events from the disposed socket
// are ignored by the epoch gate in attachSdkListeners().
function disposeSdkSocket(reason) {
  const sock = sdkSock;
  sdkSock = null;
  sdkListenersAttached = false;
  sdkTokenInUse = null;
  state.epoch += 1;
  state.sock = null;
  streaming.clear();
  subscribeInFlight = false;
  lastCanaryTickAt = 0;
  if (stableTimer) { clearTimeout(stableTimer); stableTimer = null; }
  if (sock) {
    // The SDK exposes different teardown hooks across versions; try them all
    // defensively — HSM/datasocket.min.js is known to throw on close.
    for (const m of ['autoreconnect', 'close', 'disconnect']) {
      try {
        if (typeof sock[m] !== 'function') continue;
        if (m === 'autoreconnect') sock[m](false); else sock[m]();
      } catch { /* SDK teardown is best-effort */ }
    }
  }
  // Belt-and-suspenders: purge the SDK's module cache so the next
  // `acquireSocket` call cannot receive a lingering module-scope singleton
  // still bound to the previous token.
  reloadFyersSDK();
  upstreamLog('log', `SDK singleton disposed (${reason})`);
}

function normalizeSymbols(symbols) {
  const out = [];
  const seen = new Set();
  for (const raw of symbols ?? []) {
    if (typeof raw !== 'string') continue;
    const s = raw.trim();
    // Drop malformed input — invalid requests poison the whole socket.
    if (!s || seen.has(s) || s.length > 96 || !s.includes(':') || /[\x00-\x1F\x7F]/.test(s)) continue;
    seen.add(s); out.push(s);
  }
  return out;
}

function isSocketReady(sock) {
  if (!sock) return false;
  if (typeof sock.isConnected === 'function') { try { return !!sock.isConnected(); } catch { return false; } }
  return true;
}

function callSdk(label, fn) {
  if (!isSocketReady(state.sock)) { console.warn(`[upstream] ${label} skipped: socket not connected`); return false; }
  try { fn(); return true; }
  catch (e) { console.warn(`[upstream] ${label} failed:`, String(e?.message ?? e).slice(0, 200)); return false; }
}

function subscribeUpstream(symbols) {
  const chunk = normalizeSymbols(symbols);
  if (!chunk.length) return true;
  return callSdk('subscribe', () => state.sock.subscribe(chunk, false, FYERS_DATA_CHANNEL));
}

function unsubscribeUpstream(symbols) {
  const chunk = normalizeSymbols(symbols);
  if (!chunk.length) return true;
  return callSdk('unsubscribe', () => {
    if (typeof state.sock.unsubscribe === 'function') state.sock.unsubscribe(chunk, false, FYERS_DATA_CHANNEL);
  });
}

function setSymbolUpdateMode() {
  const sock = state.sock;
  if (!sock || typeof sock.mode !== 'function') return true;
  const mode = sock.FullMode ?? sock.symbolUpdate;
  if (mode == null) return true;
  return callSdk('mode', () => sock.mode(mode, FYERS_DATA_CHANNEL));
}

function frameSymbol(frame) {
  if (!frame || typeof frame !== 'object') return null;
  const candidate = frame.symbol || frame.n || frame.sym || frame.s;
  if (typeof candidate !== 'string') return null;
  // Real tradable symbols are exchange-qualified (NSE:..., BSE:..., MCX:...).
  // Fyers control frames commonly use `s: "ok" | "error"` — skip those.
  return candidate.includes(':') ? candidate : null;
}

function handleControlFrame(frame) {
  if (!frame || typeof frame !== 'object') return false;
  if (frameSymbol(frame)) return false;
  const h = Object.prototype.hasOwnProperty;
  const hasControlShape = h.call(frame, 'code') || h.call(frame, 'stCode') || h.call(frame, 'message') || h.call(frame, 'msg')
    || frame.s === 'ok' || frame.s === 'error' || frame.stat === 'ok' || frame.stat === 'error';
  if (!hasControlShape) return false;

  const type = String(frame.type ?? '');
  const status = String(frame.s ?? frame.stat ?? '').toLowerCase();
  const codeRaw = frame.code ?? frame.stCode;
  const code = Number(codeRaw);
  const message = String(frame.message ?? frame.msg ?? '').slice(0, 300);
  const failed = status === 'error' || (Number.isFinite(code) && code !== 200) ||
    /failed|invalid|faulty|unable\s+to\s+send|request\s+not\s+valid|token/i.test(message);
  if (!failed) return true;

  console.warn('[upstream] control failure:', { type, code: Number.isFinite(code) ? code : codeRaw, message });
  if (/token|auth|401|403|-300/i.test(message) || code === 401 || code === 403 || code === -300) {
    state.token = null; teardown('auth control failure'); setUpstream('idle'); return true;
  }
  if (/sub|unsub|ful|lit|mode|connection|faulty|request/i.test(`${type} ${message}`)) {
    teardown('sdk control failure'); enterCooldown('sdk control failure');
  }
  return true;
}

function handleControlFrames(msg) {
  const frames = Array.isArray(msg) ? msg : [msg];
  let handled = false;
  for (const frame of frames) handled = handleControlFrame(frame) || handled;
  return handled;
}

function teardown(reason) {
  // Do NOT close the SDK socket here — the singleton must survive across
  // reconnect cycles (see acquireSocket). Listeners are epoch-gated.
  state.epoch++;
  state.sock = null;
  streaming.clear();
  subscribeInFlight = false;
  // Reset canary liveness timestamp — otherwise a stale value from the previous
  // session can immediately trip the LIVENESS_MS check on the next session
  // before a fresh canary tick has a chance to arrive.
  lastCanaryTickAt = 0;
  if (stableTimer) { clearTimeout(stableTimer); stableTimer = null; }
  if (reason) { lastCloseReason = reason; upstreamLog('log', `teardown (${reason})`); }
}

function enterCooldown(reason) {
  state.attempts += 1;
  const delay = computeBackoffMs();
  state.cooldownDeadline = Date.now() + delay;
  state.connectDeadline = 0;
  setUpstream('cooldown', `${reason} — retry in ${delay}ms (attempt #${state.attempts})`);
}

function beginConnect() {
  if (!state.token || !isMarketOpen()) { setUpstream('idle'); return; }
  const fullToken = `${FYERS_APP_ID}:${state.token}`;
  // Token rotated? Dispose the old SDK singleton and rebuild against the
  // new token in-process. No process.exit, no deferral — a re-login mid-
  // session resumes ticks in the same connect cycle.
  if (sdkSock && sdkTokenInUse && sdkTokenInUse !== fullToken) {
    disposeSdkSocket('token rotated');
  }
  let sock;
  try { sock = acquireSocket(fullToken); }
  catch (e) { console.warn('[upstream] constructor threw:', e?.message); enterCooldown('constructor threw'); return; }
  state.sock = sock;
  state.connectDeadline = Date.now() + CONNECT_TIMEOUT_MS;
  setUpstream('connecting');
  attachSdkListeners();
  try { sock.connect(); }
  catch (e) { console.warn('[upstream] connect() threw:', e?.message); teardown('connect() threw'); enterCooldown('connect() threw'); }
}

// Attach SDK event handlers exactly once. Handlers gate on `state.sock`
// so stale events from a torn-down connect are ignored.
function attachSdkListeners() {
  if (sdkListenersAttached || !sdkSock) return;
  sdkListenersAttached = true;
  const sock = sdkSock;
  const alive = () => sock === state.sock;

  sock.on('connect', () => {
    if (!alive()) return;
    upstreamLog('log', '🟢 connect event fired');
    // NOTE: do NOT reset state.attempts here — the socket has only proven
    // it can TCP-connect, not that it can stay live. A stability timer
    // resets attempts only after STABLE_SESSION_MS of continuous liveness
    // with at least one real tick, so rapid fail cycles correctly escalate
    // the exponential backoff instead of hammering upstream every ~2s.
    state.connectDeadline = 0; state.lastTickAt = Date.now();
    setUpstream('live');
    if (stableTimer) clearTimeout(stableTimer);
    const myEpoch = state.epoch;
    stableTimer = setTimeout(() => {
      if (state.epoch === myEpoch && state.upstream === 'live' && state.lastTickAt >= liveEnteredAt) {
        state.attempts = 0;
        upstreamLog('log', `session stable ≥${STABLE_SESSION_MS}ms — backoff reset`);
      }
      stableTimer = null;
    }, STABLE_SESSION_MS);
  });

  sock.on('message', (msg) => {
    if (!alive()) return;
    state.lastTickAt = Date.now();
    if (handleControlFrames(msg)) return;
    handleTick(msg);
  });

  sock.on('close', () => {
    if (!alive()) return;
    console.warn('🔴 upstream closed');
    teardown('close event');
    if (isMarketOpen() && state.token) enterCooldown('socket close');
    else setUpstream('idle');
  });

  sock.on('error', (err) => {
    if (!alive()) return;
    const s = String(err?.message ?? err).slice(0, 200);
    console.warn('❌ upstream error:', s);
    if (/token|auth|401|403/i.test(s)) {
      // The stored token was rejected by Fyers. Drop it, dispose the SDK
      // singleton (so a fresh module is re-required on the next connect
      // once a new token arrives), and back off exponentially instead of
      // hammering upstream every heartbeat tick.
      state.token = null;
      disposeSdkSocket('auth error');
      enterCooldown('auth error');
      return;
    }
    if (/Only one instance|Connection faulty|request not valid|Unable to send request|Mode change failed|subscription failed/i.test(s)) {
      teardown('sdk error event'); enterCooldown('sdk error event');
    }
    // Otherwise let the close handler drive cooldown.
  });
}

// Diff desired-vs-streaming, subscribe missing symbols in small chunks.
function maybeSubscribe() {
  if (state.upstream !== 'live' || !state.sock) return;
  if (subscribeInFlight) return;
  const desired = normalizeSymbols([...refCounts.keys()]);
  const missing = desired.filter((s) => !streaming.has(s));
  const extra   = [...streaming].filter((s) => !refCounts.has(s));

  if (extra.length && unsubscribeUpstream(extra)) for (const s of extra) streaming.delete(s);

  if (!missing.length) return;
  subscribeInFlight = true;
  const sock = state.sock;
  const myEpoch = state.epoch;
  let i = 0;
  const step = () => {
    if (sock !== state.sock || myEpoch !== state.epoch || state.upstream !== 'live') {
      subscribeInFlight = false; return;
    }
    const chunk = missing.slice(i, i + SUBSCRIBE_CHUNK_SIZE);
    if (!chunk.length) {
      subscribeInFlight = false;
      if (!setSymbolUpdateMode()) { teardown('mode failed'); enterCooldown('mode failed'); }
      // Reset liveness clock so the pre-subscribe pause doesn't trip the zombie killer.
      state.lastTickAt = Date.now();
      return;
    }
    if (!subscribeUpstream(chunk)) {
      subscribeInFlight = false; teardown('subscribe failed'); enterCooldown('subscribe failed'); return;
    }
    for (const s of chunk) streaming.add(s);
    i += SUBSCRIBE_CHUNK_SIZE;
    setTimeout(step, SUBSCRIBE_CHUNK_GAP_MS);
  };
  step();
}

// ─── Heartbeat ──────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  const marketOpen = isMarketOpen();

  if (!sessionInFlight && now >= state.sessionDeadline) {
    sessionInFlight = true;
    state.sessionDeadline = now + SESSION_POLL_MS;
    loadAccessToken().then((tok) => {
      if (tok === state.token) return;
      if (!tok) {
        if (state.token) { console.warn('[session] token expired — dropping upstream'); state.token = null; teardown('token expired'); setUpstream('idle'); }
        return;
      }
      const first = !state.token;
      // Validate token via a lightweight REST call before letting the
      // heartbeat wire it into the WebSocket. A bad token here would
      // otherwise trigger a reconnect storm ("Please provide valid token"
      // → teardown → idle → beginConnect → repeat every second).
      return validateFyersToken(tok).then((ok) => {
        if (!ok) {
          console.warn('[session] token validation failed — leaving upstream idle until re-login');
          state.token = null;
          if (sdkSock) disposeSdkSocket('token validation failed');
          setUpstream('idle');
          return;
        }
        state.token = tok; state.attempts = 0;
        if (first) return;
        // Rotation mid-run: drop the current upstream so beginConnect()
        // runs the next heartbeat, disposes the SDK singleton bound to
        // the old token, and rebuilds against the new one.
        console.log('[session] token rotated — reconnecting upstream');
        teardown('token rotated'); setUpstream('idle');
      });
    })
      .catch((e) => console.warn('[session]', e?.message))
      .finally(() => { sessionInFlight = false; });
  }

  if (now >= state.watchedDeadline) {
    state.watchedDeadline = now + WATCHED_REFRESH_MS; refreshWatchedSymbols();
  }

  if (now >= state.broadcastDeadline) {
    state.broadcastDeadline = now + MARKET_STATUS_BROADCAST_MS;
    const msg = { type: 'market_status', open: marketOpen };
    for (const sub of subscribers) sendJson(sub.ws, msg);
  }

  // Detect closed→open flip and wipe intraday accumulators so a symbol we've
  // been holding in `priceMap` overnight doesn't seed today's high/low with
  // yesterday's values. Runs BEFORE the early-return below.
  if (marketOpen && !wasMarketOpen) {
    upstreamLog('log', 'market opened — clearing intraday accumulators');
    priceMap.clear();
    prevLtp.clear();
    lastSnapshotSig.clear();
    lastQuoteWriteAt.clear();
  }
  wasMarketOpen = marketOpen;

  if (!marketOpen || !state.token) {
    if (state.upstream !== 'idle') { teardown('market closed or no token'); setUpstream('idle'); }
    return;
  }

  switch (state.upstream) {
    case 'idle':
      beginConnect(); break;
    case 'connecting':
      if (state.connectDeadline && now >= state.connectDeadline) {
        console.warn(`[upstream] connect timeout after ${CONNECT_TIMEOUT_MS}ms`);
        teardown('connect timeout'); enterCooldown('connect timeout');
      }
      break;
    case 'live':
      // Transport liveness: gate on the canary symbol (always subscribed,
      // ticks every second during market hours) rather than aggregate
      // `streaming.size`. Illiquid F&O going quiet no longer forces a
      // reconnect, and a half-open TCP is caught the moment canary silence
      // exceeds LIVENESS_MS.
      if (streaming.has(CANARY_SYMBOL)) {
        const silentMs = now - (lastCanaryTickAt || liveEnteredAt || now);
        if (silentMs > LIVENESS_MS) {
          console.warn(`[upstream] canary silent ${Math.round(silentMs / 1000)}s — reconnecting`);
          teardown('canary silence'); enterCooldown('canary silence'); break;
        }
      }
      maybeSubscribe();
      break;
    case 'cooldown':
      if (now >= state.cooldownDeadline) beginConnect();
      break;
  }
}, HEARTBEAT_MS);

// fyers-api-v3's HSM/datasocket.min.js throws async errors that skip the
// SDK's .on('error'). Without these handlers Railway kills the pod.
function isFyersSdkError(err) {
  return /HSM|datasocket|fyers-api-v3|Connection\s+faulty|request\s+not\s+valid|Unable\s+to\s+send\s+request/i
    .test(`${String(err?.message ?? err ?? '')}\n${String(err?.stack ?? '')}`);
}
for (const evt of ['uncaughtException', 'unhandledRejection']) {
  process.on(evt, (err) => {
    console.warn(`[${evt}]`, String(err?.message ?? err).slice(0, 300));
    if (isFyersSdkError(err) && state.sock) { teardown(evt); enterCooldown(evt); }
  });
}

// ─── Tick handling ──────────────────────────────────────────────────────
function round(n, d = 2) {
  if (n == null || !Number.isFinite(n)) return null;
  const p = Math.pow(10, d);
  return Math.round(n * p) / p;
}

function handleTick(msg) {
  if (!msg || typeof msg !== 'object') return;
  const ticks = Array.isArray(msg) ? msg : [msg];
  const updates = {};
  for (const raw of ticks) {
    const sym = frameSymbol(raw) || raw.s;
    if (!sym) continue;
    if (sym === CANARY_SYMBOL) lastCanaryTickAt = Date.now();
    if (!refCounts.has(sym)) continue;

    const ltp   = raw.ltp ?? raw.lp ?? null;
    if (ltp == null) continue;
    let   open  = raw.open_price ?? raw.op ?? null;
    let   high  = raw.high_price ?? raw.hp ?? null;
    let   low   = raw.low_price ?? raw.slp ?? raw.low ?? null;
    // NOTE: `tick.close` is actually prev_close (Fyers `cp`). Never let it
    // regress to null once we've observed a real value — otherwise change/chp
    // computes as null on subsequent ticks and the UI renders dashes.
    const rawClose = raw.prev_close_price ?? raw.cp ?? null;
    const cached   = priceMap.get(sym);
    const close    = rawClose ?? cached?.close ?? null;
    const vol   = raw.vol_traded_today ?? raw.v ?? raw.volume ?? null;

    // Index ticks often omit OHLC; compose from running stream so the
    // snapshot table has real intraday high/low instead of dashes. Intraday
    // accumulators are wiped at the closed→open transition (heartbeat) so
    // yesterday's H/L cannot leak into today's first tick.
    if (open == null) open = cached?.open ?? ltp;
    high = Math.max(high ?? cached?.high ?? ltp, ltp);
    low  = Math.min(low  ?? cached?.low  ?? ltp, ltp);

    const ch = raw.ch ?? (close != null ? ltp - close : null);
    const chp = raw.chp ?? (close && close !== 0 ? ((ltp - close) / close) * 100 : null);

    const prev = prevLtp.get(sym);
    prevLtp.set(sym, ltp);
    const dir = prev == null || ltp === prev ? 'flat' : ltp > prev ? 'up' : 'down';

    const tick = {
      symbol: sym,
      ltp: round(ltp), open: round(open), high: round(high),
      low: round(low), close: round(close), volume: vol,
      ch: round(ch), chp: round(chp, 2), dir,
      session: isMarketOpen() ? 'open' : 'closed',
      ts: Date.now(),
    };
    priceMap.set(sym, tick);
    updates[sym] = tick;
    persistServerQuote(sym, tick);
  }
  if (Object.keys(updates).length) fanout(updates);
}

function persistQuoteSnapshot(symbol, tick, now) {
  // Dedup on unchanged values — table is dashboard's source of truth on mount.
  const sig = `${tick.ltp}|${tick.open}|${tick.high}|${tick.low}|${tick.volume}|${tick.close}`;
  if (lastSnapshotSig.get(symbol) === sig) return;
  // COALESCE-aware RPC preserves EOD OHLC when index ticks omit them.
  const session = tick.session === 'open' ? 'live' : 'closed';
  const source  = tick.session === 'open' ? 'live' : 'rest';
  // Commit the dedup signature ONLY on RPC success — otherwise a single
  // failed write would permanently poison this symbol's snapshot because
  // subsequent identical ticks would be dedup-skipped forever.
  rpcWithRetry('apply_live_tick_snapshot', {
    p_symbol: symbol, p_ltp: tick.ltp,
    p_open: tick.open ?? null, p_high: tick.high ?? null, p_low: tick.low ?? null,
    p_prev_close: tick.close ?? null,
    p_volume: tick.volume ?? null,
    p_source: source, p_session: session,
    p_ts: new Date(now).toISOString(),
  }).then((ok) => {
    if (ok) lastSnapshotSig.set(symbol, sig);
  });
}

function persistServerQuote(symbol, tick) {
  const price = tick?.ltp;
  if (price == null || !Number.isFinite(price) || price <= 0) return;
  const now = Date.now();
  if (!watchedSymbols.has(symbol)) {
    const last = lastQuoteWriteAt.get(symbol) ?? 0;
    if (now - last < LAST_QUOTE_TTL_MS) return;
    lastQuoteWriteAt.set(symbol, now);
    persistQuoteSnapshot(symbol, tick, now);
    return;
  }
  // Watched (OPEN/TRIGGERED order): snapshot every tick + kick settle RPC.
  persistQuoteSnapshot(symbol, tick, now);
  triggerSettle();
}

function triggerSettle() {
  const now = Date.now();
  if (settleInFlight || now - lastSettleAt < SETTLE_THROTTLE_MS) return;
  lastSettleAt = now; settleInFlight = true;
  rpcWithRetry('settle_pending_orders', {}).finally(() => { settleInFlight = false; });
}

async function refreshWatchedSymbols() {
  try {
    const { data, error } = await supabase.rpc('open_fno_pending_quote_symbols');
    if (error) throw error;
    watchedSymbols.clear();
    for (const row of (data ?? [])) if (row?.symbol) watchedSymbols.add(row.symbol);
  } catch (e) { console.warn('[watched]', e.message); }
}

// ─── Fanout ─────────────────────────────────────────────────────────────
function sendJson(ws, obj) {
  if (ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify(obj)); } catch { /* ignore */ }
}

function broadcastStatus(status) {
  for (const sub of subscribers) sendJson(sub.ws, { type: 'status', status });
}

function fanout(updates) {
  const syms = Object.keys(updates);
  if (!syms.length) return;
  for (const sub of subscribers) {
    const slice = {};
    let has = false;
    for (const s of syms) if (sub.symbols.has(s)) { slice[s] = updates[s]; has = true; }
    if (has) sendJson(sub.ws, { type: 'tick', data: slice });
  }
}

function addSymbols(sub, syms) {
  for (const s of normalizeSymbols(syms)) {
    if (sub.symbols.has(s)) continue;
    sub.symbols.add(s);
    refCounts.set(s, (refCounts.get(s) ?? 0) + 1);
  }
}

function removeSymbols(sub, syms) {
  for (const s of syms) {
    if (!sub.symbols.delete(s)) continue;
    const n = (refCounts.get(s) ?? 1) - 1;
    if (n <= 0) { refCounts.delete(s); prevLtp.delete(s); priceMap.delete(s); }
    else refCounts.set(s, n);
  }
}

function dropSubscriber(sub) {
  if (subscribers.delete(sub)) removeSymbols(sub, [...sub.symbols]);
}

// ─── HTTP + WS server ───────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '256kb' }));
mountSyncRoute(app);
const server = createServer(app);

app.get('/health', (_req, res) => {
  const iso = (ms) => (ms ? new Date(ms).toISOString() : null);
  const now = Date.now();
  const durations = liveSessionDurationsMs.slice();
  const avg = durations.length ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length) : null;
  res.json({
    ok: true,
    upstream: state.upstream,
    hasToken: !!state.token,
    marketOpen: isMarketOpen(),
    holidayCount: holidaySet.size,
    attempts: state.attempts,
    lastTickAt: iso(state.lastTickAt),
    lastCanaryTickMsAgo: lastCanaryTickAt ? now - lastCanaryTickAt : null,
    lastCloseReason,
    liveSessionDurationsMs: durations,
    avgLiveSessionMs: avg,
    consecutiveShortLivedSessions,
    sdkTokenMatchesState: sdkTokenInUse === (state.token ? `${FYERS_APP_ID}:${state.token}` : null),
    connectDeadline: iso(state.connectDeadline),
    cooldownDeadline: iso(state.cooldownDeadline),
    subscribers: subscribers.size,
    symbols: refCounts.size,
    streaming: streaming.size,
    watched: watchedSymbols.size,
    inflightWrites,
    consecutiveRpcFailures: { ...rpcFailureCounts },
  });
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', async (req, socket, head) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return; }
    if (allowedOrigins.length && !allowedOrigins.includes(req.headers.origin || '')) {
      socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
    }
    const ticket = url.searchParams.get('ticket') || '';
    const payload = verifyTicket(ticket, FYERS_TICKET_SECRET);
    if (!payload) { socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n'); socket.destroy(); return; }
    // Prime token so first-ever connect doesn't wait for SESSION_POLL_MS.
    if (!state.token) {
      const tok = await loadAccessToken();
      if (tok) state.token = tok;
    }
    if (!state.token) { socket.write('HTTP/1.1 409 Conflict\r\n\r\nTOKEN_EXPIRED_OR_NOT_CONNECTED'); socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, (ws) => onConnection(ws, payload));
  } catch (e) { console.warn('[upgrade]', e.message); try { socket.destroy(); } catch { /* ignore */ } }
});

function onConnection(ws, _payload) {
  const sub = { ws, symbols: new Set() };
  subscribers.add(sub);
  sendJson(ws, { type: 'status', status: state.upstream === 'live' ? 'connected' : state.upstream });
  sendJson(ws, { type: 'market_status', open: isMarketOpen() });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'subscribe' && Array.isArray(msg.symbols)) {
      addSymbols(sub, msg.symbols);
      const snapshot = {};
      for (const s of msg.symbols) {
        if (typeof s !== 'string') continue;
        const t = priceMap.get(s);
        if (t) snapshot[s] = t;
      }
      if (Object.keys(snapshot).length) sendJson(ws, { type: 'init', data: snapshot, marketOpen: isMarketOpen() });
    } else if (msg.type === 'unsubscribe' && Array.isArray(msg.symbols)) {
      removeSymbols(sub, msg.symbols.filter((s) => typeof s === 'string'));
    } else if (msg.type === 'ping') {
      sendJson(ws, { type: 'pong' });
    }
  });

  ws.on('close', () => dropSubscriber(sub));
  ws.on('error', () => dropSubscriber(sub));
}

// ─── Boot ───────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`\n🚀 Fyers live worker listening on :${PORT}`);
  // Pin canary symbol with a permanent ref-count so maybeSubscribe() always
  // keeps it subscribed upstream regardless of browser subscriptions.
  refCounts.set(CANARY_SYMBOL, (refCounts.get(CANARY_SYMBOL) ?? 0) + 1);
  await refreshHolidays();
  setInterval(refreshHolidays, HOLIDAYS_REFRESH_MS).unref();
  const tok = await loadAccessToken();
  if (tok) state.token = tok;
  else console.warn('⚠️  No valid Fyers session yet. Heartbeat polls every', SESSION_POLL_MS, 'ms.');
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    console.log(`\n${sig} received — shutting down`);
    for (const sub of subscribers) { try { sub.ws.close(); } catch { /* ignore */ } }
    teardown('shutdown');
    // Drain up to ~2s of in-flight snapshot / settle writes so a Railway
    // redeploy doesn't silently drop a tick that was mid-RPC at exit time.
    const drainDeadline = Date.now() + 2000;
    const finish = () => {
      server.close(() => process.exit(0));
      setTimeout(() => process.exit(0), 1000).unref();
    };
    const waitDrain = () => {
      if (inflightWrites <= 0 || Date.now() > drainDeadline) {
        if (inflightWrites > 0) console.warn(`[shutdown] draining timeout, ${inflightWrites} writes in flight`);
        finish();
      } else {
        setTimeout(waitDrain, 50);
      }
    };
    waitDrain();
  });
}
