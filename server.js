// FRONTLINE — servidor de salas (HTTP estático + WebSocket propio, SIN dependencias: solo Node 18+)
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const MAX_ROOM = +process.env.MAX_ROOM || 8;
const MAX_FRAME = 64 * 1024;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const RELAY = new Set(['st', 'shoot', 'bots', 'bfire', 'bkill', 'bhit', 'pdmg', 'pdead', 'expl', 'vsnap', 'bsnap', 'vpark', 'restart', 'win']);
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.ico': 'image/x-icon', '.svg': 'image/svg+xml' };

/* ---------- WebSocket mínimo (RFC 6455) ---------- */
class Conn extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket; this.buf = Buffer.alloc(0); this.readyState = 1; this.frags = []; this.fragOp = 0; this.alive = true;
    socket.on('data', (d) => { this.buf = Buffer.concat([this.buf, d]); this.parse(); });
    socket.on('close', () => this.finish());
    socket.on('end', () => this.finish());
    socket.on('error', () => this.finish());
  }
  finish() { if (this.readyState === 3) return; this.readyState = 3; this.emit('close'); }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 128), op = b[0] & 15, masked = !!(b[1] & 128);
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = b.readUInt32BE(2) * 4294967296 + b.readUInt32BE(6); off = 10; }
      if (len > MAX_FRAME) return this.close(1009);
      if (!masked) return this.close(1002);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) return this.close(1000);
      if (op === 9) { this.frame(10, data); continue; }
      if (op === 10) { this.alive = true; continue; }
      if (op === 1 || op === 2) { this.frags = [data]; this.fragOp = op; } else if (op === 0) this.frags.push(data); else continue;
      if (this.frags.reduce((a, x) => a + x.length, 0) > MAX_FRAME) return this.close(1009);
      if (fin) { const full = Buffer.concat(this.frags); this.frags = []; if (this.fragOp === 1) this.emit('message', full.toString('utf8')); }
    }
  }
  frame(op, payload) {
    if (this.readyState !== 1) return;
    const n = payload.length; let h;
    if (n < 126) h = Buffer.from([128 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
    try { this.socket.write(Buffer.concat([h, payload])); } catch { /* socket cerrado */ }
  }
  send(str) { if (this.socket.writableLength > 1024 * 1024) return; this.frame(1, Buffer.from(str)); }
  ping() { this.frame(9, Buffer.alloc(0)); }
  close(code = 1000) {
    if (this.readyState !== 1) return;
    const p = Buffer.alloc(2); p.writeUInt16BE(code); this.frame(8, p); this.readyState = 2;
    try { this.socket.end(); } catch { /* nada */ }
    setTimeout(() => { try { this.socket.destroy(); } catch { /* nada */ } this.finish(); }, 500);
  }
}

const server = http.createServer((req, res) => {
  let u = (req.url || '/').split('?')[0];
  try { u = decodeURIComponent(u); } catch { res.writeHead(400); return res.end('400'); }
  if (u === '/') u = '/index.html';
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  const fp = path.join(PUB, u);
  if (!fp.startsWith(PUB)) { res.writeHead(403); return res.end('403'); }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); return res.end('404'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  onConnection(new Conn(socket));
});

/* ---------- Salas ---------- */
const rooms = new Map();
let nextId = 1;
const clean = (s) => String(s || 'Soldado').replace(/[^\p{L}\p{N} _\-.]/gu, '').trim().slice(0, 14) || 'Soldado';
const normCode = (c) => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
function newCode() {
  for (let k = 0; k < 200; k++) {
    let c = ''; for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}
const sendTo = (p, m) => { if (p.ws.readyState === 1) p.ws.send(typeof m === 'string' ? m : JSON.stringify(m)); };
const roomBcast = (room, m, except) => { const s = JSON.stringify(m); for (const p of room.players.values()) if (p !== except) sendTo(p, s); };

function onConnection(ws) {
  const p = { id: nextId++, ws, name: 'Soldado', room: null };
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'create' || m.t === 'join') {
      if (p.room) return;
      p.name = clean(m.name);
      let room;
      if (m.t === 'create') {
        const code = newCode(); if (!code) return sendTo(p, { t: 'err', msg: 'No se pudo crear la sala' });
        room = { code, host: p.id, seed: crypto.randomInt(1, 2 ** 30), players: new Map() };
        rooms.set(code, room);
      } else {
        room = rooms.get(normCode(m.code));
        if (!room) return sendTo(p, { t: 'err', msg: 'No existe una sala con ese código' });
        if (room.players.size >= MAX_ROOM) return sendTo(p, { t: 'err', msg: 'La sala está llena (' + MAX_ROOM + ')' });
      }
      p.room = room;
      const others = [...room.players.values()].map((q) => ({ id: q.id, n: q.name }));
      room.players.set(p.id, p);
      sendTo(p, { t: 'room', code: room.code, id: p.id, host: room.host === p.id, hostId: room.host, seed: room.seed, players: others });
      roomBcast(room, { t: 'joined', id: p.id, n: p.name }, p);
      return;
    }
    const room = p.room;
    if (!room || !RELAY.has(m.t)) return;
    const to = m.to; delete m.to; m.id = p.id;
    const s = JSON.stringify(m);
    if (to === 'host') { const h = room.players.get(room.host); if (h && h !== p) sendTo(h, s); }
    else if (typeof to === 'number') { const q = room.players.get(to); if (q) sendTo(q, s); }
    else for (const q of room.players.values()) if (q !== p) sendTo(q, s);
  });
  ws.on('close', () => {
    const room = p.room; if (!room) return;
    room.players.delete(p.id);
    if (!room.players.size) { rooms.delete(room.code); return; }
    roomBcast(room, { t: 'left', id: p.id });
    if (room.host === p.id) {
      room.host = room.players.keys().next().value;
      roomBcast(room, { t: 'host', hostId: room.host });
    }
  });
}

// Latido para detectar conexiones muertas (móviles que pierden señal)
const hb = setInterval(() => {
  for (const room of rooms.values()) for (const p of room.players.values()) {
    if (!p.ws.alive) { try { p.ws.socket.destroy(); } catch { /* nada */ } continue; }
    p.ws.alive = false; p.ws.ping();
  }
}, 15000);
hb.unref();

if (require.main === module) server.listen(PORT, () => console.log('FRONTLINE listo en http://localhost:' + PORT));
module.exports = { server, rooms };
