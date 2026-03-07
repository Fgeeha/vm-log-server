/**
 * VM Log Server — интеграция с Synology NAS
 *
 * Настройка: cp .env.example .env  →  заполнить  →  node server.js
 */
'use strict';

try { require('dotenv').config(); } catch (_) {}

const dgram   = require('dgram');
const fs      = require('fs');
const path    = require('path');
const http    = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
let Database = null;
try { Database = require('better-sqlite3'); } catch (_) {}

const CONFIG = {
  synologyHost:    process.env.SYNOLOGY_HOST       || '192.168.1.100',
  synologyPort:    process.env.SYNOLOGY_PORT        || '5000',
  synologyHttps:   process.env.SYNOLOGY_HTTPS       === 'true',
  synologyUser:    process.env.SYNOLOGY_USER        || 'admin',
  synologyPass:    process.env.SYNOLOGY_PASSWORD    || '',
  syslogPort:      parseInt(process.env.SYSLOG_PORT  || '514'),
  syslogHost:      process.env.SYSLOG_HOST           || '0.0.0.0',
  syslogFormat:    process.env.SYSLOG_FORMAT         || 'ietf',
  httpPort:        parseInt(process.env.HTTP_PORT    || '3000'),
  corsOrigin:      process.env.CORS_ORIGIN           || '*',
  apiToken:        process.env.API_TOKEN             || '',
  allowedIps:      process.env.ALLOWED_IPS           || '*',
  pollEnabled:     process.env.POLL_ENABLED          !== 'false',
  pollIntervalMs:  parseInt(process.env.POLL_INTERVAL_MS || '30000'),
  pollCsvPath:     process.env.NAS_CSV_PATH          || './logs/nas_log.csv',
  pollCsvEncoding: process.env.POLL_CSV_ENCODING     || 'utf8',
  maxEvents:       parseInt(process.env.MAX_EVENTS   || '10000'),
  dbEnabled:       process.env.DB_ENABLED            !== 'false',
  dbPath:          process.env.DB_PATH               || './data/events.db',
  logLevel:        process.env.LOG_LEVEL             || 'info',
  logFile:         process.env.LOG_FILE              || '',
};

// ─── ЛОГГЕР ──────────────────────────────────────────────────────────────────
const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const curLevel = LEVELS[CONFIG.logLevel] ?? 1;

function log(level, ...args) {
  if ((LEVELS[level] ?? 1) < curLevel) return;
  const line = `[${new Date().toISOString()}] [${level.toUpperCase().padEnd(5)}] ${args.join(' ')}`;
  console.log(line);
  if (CONFIG.logFile) {
    try {
      fs.mkdirSync(path.dirname(path.resolve(CONFIG.logFile)), { recursive: true });
      fs.appendFileSync(CONFIG.logFile, line + '\n');
    } catch (_) {}
  }
}

log('info', '╔══════════════════════════════════════════════════╗');
log('info', '║          VM Log Server — Synology NAS            ║');
log('info', `║  NAS      : ${CONFIG.synologyHost}:${CONFIG.synologyPort}`);
log('info', `║  Syslog   : ${CONFIG.syslogHost}:${CONFIG.syslogPort}`);
log('info', `║  HTTP/WS  : http://localhost:${CONFIG.httpPort}`);
log('info', `║  CSV Poll : ${CONFIG.pollCsvPath}`);
log('info', `║  DB       : ${CONFIG.dbEnabled ? CONFIG.dbPath : 'disabled'}`);
log('info', '╚══════════════════════════════════════════════════╝');

if (!process.env.SYNOLOGY_PASSWORD) {
  log('warn', 'SYNOLOGY_PASSWORD не задан — скопируйте .env.example в .env и заполните');
}

// ─── RING-BUFFER ─────────────────────────────────────────────────────────────
const events = [];
let   eventIdCounter = 0;
const stats = { total: 0, syslog: 0, csv: 0, lastUpdate: null };
let db = null;
let insertEventStmt = null;

function initDatabase() {
  if (!CONFIG.dbEnabled) {
    log('info', 'SQLite отключен (DB_ENABLED=false), работаем только в памяти');
    return;
  }
  if (!Database) {
    log('warn', 'Модуль better-sqlite3 не установлен, работаем только в памяти');
    log('warn', 'Установите зависимости: npm install');
    return;
  }
  try {
    const absDbPath = path.resolve(CONFIG.dbPath);
    fs.mkdirSync(path.dirname(absDbPath), { recursive: true });
    db = new Database(absDbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        journal TEXT,
        time TEXT,
        ip TEXT,
        user TEXT,
        event TEXT,
        filetype TEXT,
        size TEXT,
        path TEXT,
        source TEXT,
        raw TEXT,
        receivedAt TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(receivedAt);
      CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
    `);
    insertEventStmt = db.prepare(`
      INSERT INTO events (journal, time, ip, user, event, filetype, size, path, source, raw, receivedAt)
      VALUES (@journal, @time, @ip, @user, @event, @filetype, @size, @path, @source, @raw, @receivedAt)
    `);
    log('info', `SQLite подключена: ${absDbPath}`);
  } catch (err) {
    db = null;
    insertEventStmt = null;
    log('error', `SQLite init: ${err.message}`);
  }
}

function loadRecentEventsFromDb() {
  if (!db) return;
  try {
    const rows = db.prepare(`
      SELECT id, journal, time, ip, user, event, filetype, size, path, source, raw, receivedAt
      FROM events
      ORDER BY id DESC
      LIMIT ?
    `).all(CONFIG.maxEvents);
    rows.reverse();
    for (const row of rows) {
      events.push({
        id: row.id,
        journal: row.journal,
        time: row.time,
        ip: row.ip,
        user: row.user,
        event: row.event,
        filetype: row.filetype,
        size: row.size,
        path: row.path,
        source: row.source,
        raw: row.raw,
        receivedAt: row.receivedAt,
      });
      eventIdCounter = Math.max(eventIdCounter, row.id);
      if (row.source === 'syslog') stats.syslog++;
      if (row.source === 'csv' || row.source === 'upload') stats.csv++;
    }
    stats.total = events.length;
    stats.lastUpdate = events.length ? events[events.length - 1].receivedAt : null;
    if (events.length) log('info', `SQLite: восстановлено ${events.length} событий из БД`);
  } catch (err) {
    log('error', `SQLite load: ${err.message}`);
  }
}

function addEvent(ev) {
  const receivedAt = new Date().toISOString();
  let eventId = ++eventIdCounter;

  if (insertEventStmt) {
    try {
      const info = insertEventStmt.run({
        journal: ev.journal || '',
        time: ev.time || '',
        ip: ev.ip || '',
        user: ev.user || '',
        event: ev.event || '',
        filetype: ev.filetype || '',
        size: ev.size || '',
        path: ev.path || '',
        source: ev.source || '',
        raw: ev.raw || '',
        receivedAt,
      });
      eventId = Number(info.lastInsertRowid);
      eventIdCounter = Math.max(eventIdCounter, eventId);
    } catch (err) {
      log('error', `SQLite insert: ${err.message}`);
    }
  }

  ev.id         = eventId;
  ev.receivedAt = receivedAt;
  events.push(ev);
  if (events.length > CONFIG.maxEvents) events.shift();
  stats.total++;
  stats.lastUpdate = ev.receivedAt;
  broadcast({ type: 'event', data: ev });
}

initDatabase();
loadRecentEventsFromDb();

// ─── WEBSOCKET ───────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });
const clients = new Set();

wss.on('connection', (ws, req) => {
  const clientIp = req.socket.remoteAddress || '';

  if (CONFIG.allowedIps !== '*') {
    const allowed = CONFIG.allowedIps.split(',').map(s => s.trim());
    if (!allowed.some(a => clientIp.includes(a))) {
      log('warn', `WS отклонён: ${clientIp}`);
      ws.close(1008, 'Forbidden');
      return;
    }
  }

  clients.add(ws);
  log('info', `WS подключён: ${clientIp} (всего: ${clients.size})`);
  ws.send(JSON.stringify({ type: 'snapshot', data: events.slice(-500), stats }));

  ws.on('close', () => { clients.delete(ws); log('debug', `WS отключился: ${clientIp}`); });
  ws.on('error', err => log('error', `WS (${clientIp}): ${err.message}`));
});

function broadcast(msg) {
  const raw = JSON.stringify(msg);
  for (const ws of clients) if (ws.readyState === 1) ws.send(raw);
}

// ─── HTTP MIDDLEWARE ──────────────────────────────────────────────────────────
app.use(express.json({ limit: '50mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin',  CONFIG.corsOrigin);
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-API-Token');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function auth(req, res, next) {
  if (!CONFIG.apiToken) return next();
  const t = req.headers['x-api-token'] || req.query.token;
  if (t !== CONFIG.apiToken) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ─── HTTP ROUTES ─────────────────────────────────────────────────────────────
app.get('/api/status', auth, (req, res) => res.json({
  ok: true, uptime: process.uptime(), stats, eventsInMemory: events.length,
  config: {
    synologyHost: CONFIG.synologyHost, syslogPort: CONFIG.syslogPort,
    httpPort: CONFIG.httpPort, pollEnabled: CONFIG.pollEnabled,
    pollIntervalMs: CONFIG.pollIntervalMs, pollCsvPath: CONFIG.pollCsvPath,
  },
}));

app.get('/api/events', auth, (req, res) => {
  let r = [...events];
  const { user, event, ip, from, to, q, limit = 1000, offset = 0 } = req.query;
  if (user)  r = r.filter(e => e.user  === user);
  if (event) r = r.filter(e => e.event === event);
  if (ip)    r = r.filter(e => e.ip    === ip);
  if (from)  r = r.filter(e => e.time  >= from);
  if (to)    r = r.filter(e => e.time  <= to);
  if (q)     r = r.filter(e => JSON.stringify(e).toLowerCase().includes(q.toLowerCase()));
  r = r.reverse().slice(Number(offset), Number(offset) + Number(limit));
  res.json({ total: events.length, returned: r.length, events: r });
});

app.post('/api/upload-csv', auth, express.text({ type: '*/*', limit: '50mb' }), (req, res) => {
  try {
    const parsed = parseCSV(req.body);
    parsed.forEach(e => { e.source = 'upload'; addEvent(e); stats.csv++; });
    log('info', `CSV upload: ${parsed.length} событий`);
    res.json({ ok: true, imported: parsed.length });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.get('/', (req, res) => {
  const p = path.join(__dirname, 'client.html');
  if (fs.existsSync(p)) return res.sendFile(p);
  res.send('<h2>VM Log Server запущен. Откройте client.html в браузере.</h2>');
});

// ─── SYSLOG UDP ───────────────────────────────────────────────────────────────
function parseSyslog(raw) {
  const str = raw.toString(CONFIG.pollCsvEncoding).trim();
  const m = re => { const r = str.match(re); return r ? r[1] : null; };
  const ev = { journal: 'Syslog', raw: str, source: 'syslog' };

  ev.time     = m(/(\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2})/)
             || m(/^<\d+>(\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2})/)
             || new Date().toISOString().slice(0, 19).replace('T', ' ');
  ev.user     = m(/user[=:\s]+([^\s,;"]+)/i)              || 'system';
  ev.ip       = m(/(?:ip|from)[=:\s]+([\d.]+)/i)          || '';
  ev.path     = m(/(?:file|path|object)[=:\s]+([^\s,;"]+)/i) || '';
  ev.event    = capitalize(m(/(?:action|event|op)[=:\s]+([^\s,;"]+)/i) || detectEvent(str));
  ev.size     = '';
  ev.filetype = ev.path ? (ev.path.includes('.') ? 'Файл' : 'Папка') : '';
  return ev;
}

function detectEvent(s) {
  const l = s.toLowerCase();
  if (l.includes('delet') || l.includes('удал'))  return 'Удалить';
  if (l.includes('write') || l.includes('запис'))  return 'Запись';
  if (l.includes('read')  || l.includes('чтени'))  return 'Чтение';
  if (l.includes('creat') || l.includes('созда'))  return 'Создать';
  if (l.includes('renam') || l.includes('переим')) return 'Переимен.';
  return 'Событие';
}

function capitalize(s) { return s ? s[0].toUpperCase() + s.slice(1).toLowerCase() : s; }

const udpServer = dgram.createSocket('udp4');

udpServer.on('message', (msg, rinfo) => {
  try {
    const ev = parseSyslog(msg);
    if (!ev.ip) ev.ip = rinfo.address;
    addEvent(ev);
    stats.syslog++;
    log('debug', `Syslog: ${rinfo.address} → ${ev.event} ${ev.user} ${ev.path}`);
  } catch (err) {
    log('error', `Syslog parse: ${err.message}`);
  }
});

udpServer.on('error', err => {
  log('error', `Syslog UDP: ${err.message}`);
  if (err.code === 'EACCES') {
    log('warn', 'Порт 514 требует root. Решения:');
    log('warn', '  1) sudo node server.js');
    log('warn', '  2) Задайте SYSLOG_PORT=5514 в .env, затем:');
    log('warn', '     sudo iptables -t nat -A PREROUTING -p udp --dport 514 -j REDIRECT --to-port 5514');
  }
});

udpServer.bind(CONFIG.syslogPort, CONFIG.syslogHost, () => {
  log('info', `Syslog UDP слушает ${CONFIG.syslogHost}:${CONFIG.syslogPort}`);
});

// ─── CSV POLLING ─────────────────────────────────────────────────────────────
let lastMtime     = 0;
let lastLineCount = 0;

function parseCSV(text) {
  const lines = text.trim().split('\n').filter(Boolean);
  if (lines.length < 2) return [];

  let delim = ',';
  for (const d of [',', ';', '\t'])
    if ((lines[0].match(new RegExp('\\' + d, 'g')) || []).length > 2) { delim = d; break; }

  const hdrs = lines[0].split(delim).map(h => h.trim().replace(/"/g, '').toLowerCase());
  const fi = (...c) => { for (const x of c) { const i = hdrs.indexOf(x); if (i >= 0) return i; const p = hdrs.findIndex(h => h.includes(x)); if (p >= 0) return p; } return -1; };

  const idx = {
    journal:  fi('журнал','journal','log'),
    time:     fi('время','time','дата','date','timestamp'),
    ip:       fi('ip','ip-адрес','ipaddress'),
    user:     fi('пользователь','user','username'),
    event:    fi('событие','event','action'),
    filetype: fi('файл/папка','тип объекта','filetype'),
    size:     fi('размер файла','размер','size'),
    path:     fi('имя файла','путь','path','filename'),
  };

  const get = (row, k) => idx[k] >= 0 ? (row[idx[k]] || '').trim().replace(/"/g, '') : '';

  const result = [];
  for (let i = 1; i < lines.length; i++) {
    const row = splitLine(lines[i], delim);
    if (!row.some(c => c.trim())) continue;
    result.push({
      journal:  get(row, 'journal')  || 'SMB',
      time:     get(row, 'time'),
      ip:       get(row, 'ip'),
      user:     get(row, 'user'),
      event:    get(row, 'event'),
      filetype: get(row, 'filetype'),
      size:     get(row, 'size'),
      path:     get(row, 'path'),
      source:   'csv',
    });
  }
  return result;
}

function splitLine(line, d) {
  const r = []; let cur = '', q = false;
  for (const ch of line) {
    if (ch === '"') q = !q;
    else if (ch === d && !q) { r.push(cur); cur = ''; }
    else cur += ch;
  }
  r.push(cur);
  return r;
}

function pollCSV() {
  if (!CONFIG.pollEnabled) return;
  if (!fs.existsSync(CONFIG.pollCsvPath)) return;
  try {
    const stat = fs.statSync(CONFIG.pollCsvPath);
    if (stat.mtimeMs <= lastMtime) return;
    const text  = fs.readFileSync(CONFIG.pollCsvPath, CONFIG.pollCsvEncoding);
    const lines = text.trim().split('\n');
    if (lastLineCount === 0) {
      const parsed = parseCSV(text);
      parsed.forEach(e => { addEvent(e); stats.csv++; });
      log('info', `Poll: загружено ${parsed.length} событий`);
    } else {
      const newL = lines.slice(lastLineCount);
      if (newL.length > 0) {
        const parsed = parseCSV([lines[0], ...newL].join('\n'));
        parsed.forEach(e => { addEvent(e); stats.csv++; });
        log('info', `Poll: +${parsed.length} новых событий`);
      }
    }
    lastLineCount = lines.length;
    lastMtime     = stat.mtimeMs;
    broadcast({ type: 'poll_update', stats });
  } catch (err) {
    log('error', `Poll: ${err.message}`);
  }
}

if (CONFIG.pollEnabled) {
  pollCSV();
  setInterval(pollCSV, CONFIG.pollIntervalMs);
  log('info', `Poll: ${CONFIG.pollCsvPath} каждые ${CONFIG.pollIntervalMs / 1000}с`);
}

// ─── СТАРТ ───────────────────────────────────────────────────────────────────
server.listen(CONFIG.httpPort, () => {
  log('info', `HTTP + WebSocket запущен: http://localhost:${CONFIG.httpPort}`);
});

function shutdown(sig) {
  log('info', `${sig} получен, завершение...`);
  udpServer.close();
  server.close(() => { log('info', 'Сервер остановлен'); process.exit(0); });
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
