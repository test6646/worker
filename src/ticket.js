// HMAC ticket verifier. Byte-compatible with supabase/functions/fyers-ws-ticket:
//   ticket = base64url(JSON.stringify({sub, exp})) + "." + base64url(HMAC-SHA256(secret, body))
//   exp is unix seconds.
import crypto from 'node:crypto';

function b64urlToBuf(s) {
  const pad = '='.repeat((4 - (s.length % 4)) % 4);
  return Buffer.from((s + pad).replace(/-/g, '+').replace(/_/g, '/'), 'base64');
}

function b64url(buf) {
  return Buffer.from(buf).toString('base64')
    .replace(/=+$/g, '').replace(/\+/g, '-').replace(/\//g, '_');
}

export function verifyTicket(ticket, secret) {
  if (!ticket || !secret) return null;
  const [body, sig] = String(ticket).split('.');
  if (!body || !sig) return null;
  const expected = b64url(crypto.createHmac('sha256', secret).update(body).digest());
  // Constant-time compare
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  let payload;
  try { payload = JSON.parse(b64urlToBuf(body).toString('utf8')); }
  catch { return null; }
  if (!payload || typeof payload.exp !== 'number') return null;
  if (payload.exp * 1000 < Date.now()) return null;
  return payload; // { sub, exp }
}