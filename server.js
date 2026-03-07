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

function resolveAppPath(p) {
  if (!p) return p;
  return path.isAbsolute(p) ? p : path.resolve(__dirname, p);
}

CONFIG.pollCsvPath = resolveAppPath(CONFIG.pollCsvPath);
CONFIG.dbPath = resolveAppPath(CONFIG.dbPath);
CONFIG.logFile = resolveAppPath(CONFIG.logFile);

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

function makeDedupeKey(ev) {
  const src = String(ev.source || '').toLowerCase();
  if (src !== 'csv' && src !== 'upload') return null;
  const norm = v => String(v || '').trim().toLowerCase();
  return [
    src,
    norm(ev.journal),
    norm(ev.time),
    norm(ev.ip),
    norm(ev.user),
    norm(ev.event),
    norm(ev.filetype),
    norm(ev.size),
    norm(ev.path),
    norm(ev.raw),
  ].join('|');
}

function normalizeForSearch(value) {
  return String(value || '').normalize('NFKC').toLocaleLowerCase('ru-RU');
}

function includesNormalized(haystack, needle) {
  const q = normalizeForSearch(needle).trim();
  if (!q) return true;
  return normalizeForSearch(haystack).includes(q);
}

function parseTimestampMs(value) {
  if (value == null) return 0;
  const raw = String(value).trim();
  if (!raw) return 0;
  const normalized = raw.replace(/\//g, '-');
  const ms = Date.parse(normalized);
  return Number.isFinite(ms) ? ms : 0;
}

function eventTimestampMs(eventTime, receivedAt) {
  // Prefer original event time from source logs; fallback to ingestion time.
  return parseTimestampMs(eventTime) || parseTimestampMs(receivedAt);
}

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
    db.function('icontains', { deterministic: true }, (value, query) =>
      includesNormalized(value, query) ? 1 : 0
    );
    db.function('event_ts', { deterministic: true }, (timeValue, receivedAtValue) =>
      eventTimestampMs(timeValue, receivedAtValue)
    );
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
        dedupeKey TEXT,
        receivedAt TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(receivedAt);
      CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
    `);
    const cols = db.prepare(`PRAGMA table_info(events)`).all().map(c => c.name);
    if (!cols.includes('dedupeKey')) {
      db.exec(`ALTER TABLE events ADD COLUMN dedupeKey TEXT`);
    }
    db.exec(`
      UPDATE events
      SET dedupeKey = lower(
        coalesce(source,'') || '|' ||
        coalesce(journal,'') || '|' ||
        coalesce(time,'') || '|' ||
        coalesce(ip,'') || '|' ||
        coalesce(user,'') || '|' ||
        coalesce(event,'') || '|' ||
        coalesce(filetype,'') || '|' ||
        coalesce(size,'') || '|' ||
        coalesce(path,'') || '|' ||
        coalesce(raw,'')
      )
      WHERE (source='csv' OR source='upload')
        AND (dedupeKey IS NULL OR dedupeKey='');
    `);
    db.exec(`
      DELETE FROM events
      WHERE id IN (
        SELECT e1.id
        FROM events e1
        JOIN events e2
          ON e1.dedupeKey = e2.dedupeKey
         AND e1.id > e2.id
        WHERE e1.dedupeKey IS NOT NULL
          AND e1.dedupeKey != ''
      );
    `);
    db.exec(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedupe
      ON events(dedupeKey)
      WHERE dedupeKey IS NOT NULL AND dedupeKey != '';
    `);
    insertEventStmt = db.prepare(`
      INSERT OR IGNORE INTO events (journal, time, ip, user, event, filetype, size, path, source, raw, dedupeKey, receivedAt)
      VALUES (@journal, @time, @ip, @user, @event, @filetype, @size, @path, @source, @raw, @dedupeKey, @receivedAt)
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
  const dedupeKey = makeDedupeKey(ev);

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
        dedupeKey: dedupeKey || null,
        receivedAt,
      });
      if (info.changes === 0) return false;
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
  return true;
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
    dbEnabled: CONFIG.dbEnabled, dbPath: CONFIG.dbPath,
  },
}));

app.get('/api/events', auth, (req, res) => {
  const { user = '', event = '', ip = '', source = '', from = '', to = '', q = '' } = req.query;
  const limit = Math.max(1, Math.min(Number(req.query.limit || 50), 500));
  const offset = Math.max(0, Number(req.query.offset || 0));
  const sortMap = {
    id: 'id',
    journal: 'journal',
    time: 'time',
    ip: 'ip',
    user: 'user',
    event: 'event',
    filetype: 'filetype',
    size: 'size',
    source: 'source',
    path: 'path',
    receivedAt: 'receivedAt',
  };
  const sortCol = sortMap[String(req.query.sort || 'time')] || 'time';
  const sortDir = String(req.query.dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';

  if (db) {
    try {
      const where = [];
      const params = {};
      if (user) { where.push('user = @user'); params.user = user; }
      if (event) { where.push('event = @event'); params.event = event; }
      if (ip) { where.push('ip = @ip'); params.ip = ip; }
      if (source) { where.push('source = @source'); params.source = source; }
      const fromTs = from ? parseTimestampMs(from) : 0;
      const toTs = to ? parseTimestampMs(to) : 0;
      if (from && fromTs) { where.push('event_ts(time, receivedAt) >= @fromTs'); params.fromTs = fromTs; }
      if (to && toTs) { where.push('event_ts(time, receivedAt) <= @toTs'); params.toTs = toTs; }
      if (q) {
        where.push(`(
          icontains(journal, @q) = 1 OR
          icontains(time, @q) = 1 OR
          icontains(ip, @q) = 1 OR
          icontains(user, @q) = 1 OR
          icontains(event, @q) = 1 OR
          icontains(filetype, @q) = 1 OR
          icontains(size, @q) = 1 OR
          icontains(path, @q) = 1 OR
          icontains(source, @q) = 1 OR
          icontains(receivedAt, @q) = 1 OR
          icontains(raw, @q) = 1
        )`);
        params.q = String(q);
      }
      const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
      const total = db.prepare(`SELECT COUNT(*) AS c FROM events ${whereSql}`).get(params).c;
      const summary = db.prepare(`
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN (
            coalesce(event,'') LIKE '%удал%' OR
            coalesce(event,'') LIKE '%Удал%' OR
            coalesce(event,'') LIKE '%УДАЛ%' OR
            lower(coalesce(event,'')) LIKE '%delet%' OR
            lower(coalesce(event,'')) LIKE '%remove%'
          ) THEN 1 ELSE 0 END) AS deletes,
          COUNT(DISTINCT CASE WHEN coalesce(ip,'') != '' THEN ip END) AS uniqueIps,
          COUNT(DISTINCT CASE WHEN coalesce(user,'') != '' THEN user END) AS uniqueUsers
        FROM events
        ${whereSql}
      `).get(params);
      const orderSql = `ORDER BY ${sortCol} ${sortDir}, id DESC`;
      const rows = db.prepare(`
        SELECT id, journal, time, ip, user, event, filetype, size, path, source, raw, receivedAt
        FROM events
        ${whereSql}
        ${orderSql}
        LIMIT @limit OFFSET @offset
      `).all({ ...params, limit, offset });
      return res.json({
        total,
        returned: rows.length,
        events: rows,
        summary: {
          total: Number(summary.total || 0),
          deletes: Number(summary.deletes || 0),
          uniqueIps: Number(summary.uniqueIps || 0),
          uniqueUsers: Number(summary.uniqueUsers || 0),
        },
      });
    } catch (err) {
      log('error', `API /events (db): ${err.message}`);
      return res.status(500).json({ error: 'DB query failed' });
    }
  }

  let r = [...events];
  if (user)  r = r.filter(e => e.user  === user);
  if (event) r = r.filter(e => e.event === event);
  if (ip)    r = r.filter(e => e.ip    === ip);
  if (source) r = r.filter(e => e.source === source);
  const ts = (eventTime, receivedAt) => eventTimestampMs(eventTime, receivedAt);
  if (from) {
    const fromTs = parseTimestampMs(from);
    if (fromTs) r = r.filter(e => ts(e.time, e.receivedAt) >= fromTs);
  }
  if (to) {
    const toTs = parseTimestampMs(to);
    if (toTs) r = r.filter(e => ts(e.time, e.receivedAt) <= toTs);
  }
  if (q)     r = r.filter(e => includesNormalized(JSON.stringify(e), q));
  const filtered = r;
  const normDelete = ev => {
    const e = String(ev || '').toLowerCase();
    return e.includes('удал') || e.includes('delet') || e.includes('remove');
  };
  const getField = (obj, field) => {
    const v = obj[field];
    return v == null ? '' : String(v);
  };
  filtered.sort((a, b) => {
    let av = getField(a, sortCol);
    let bv = getField(b, sortCol);
    if (sortCol === 'id') {
      av = Number(a.id || 0);
      bv = Number(b.id || 0);
    }
    if (sortCol === 'size') {
      const parseSize = s => {
        const m = String(s).match(/([\d.]+)\s*(bytes?|kb|mb|gb|байт|кб|мб|гб)/i);
        if (!m) return 0;
        const v = parseFloat(m[1]);
        const u = m[2].toLowerCase();
        if (u.startsWith('k') || u.startsWith('к')) return v * 1024;
        if (u.startsWith('m') || u.startsWith('м')) return v * 1024 * 1024;
        if (u.startsWith('g') || u.startsWith('г')) return v * 1024 * 1024 * 1024;
        return v;
      };
      av = parseSize(av);
      bv = parseSize(bv);
    }
    if (sortCol === 'time' || sortCol === 'receivedAt') {
      av = new Date(String(av).replace(/\//g, '-')).getTime() || 0;
      bv = new Date(String(bv).replace(/\//g, '-')).getTime() || 0;
    }
    if (av < bv) return sortDir === 'ASC' ? -1 : 1;
    if (av > bv) return sortDir === 'ASC' ? 1 : -1;
    return Number(b.id || 0) - Number(a.id || 0);
  });
  const total = filtered.length;
  const summary = {
    total,
    deletes: filtered.filter(e => normDelete(e.event)).length,
    uniqueIps: new Set(filtered.map(e => e.ip).filter(Boolean)).size,
    uniqueUsers: new Set(filtered.map(e => e.user).filter(Boolean)).size,
  };
  const rows = filtered.slice(offset, offset + limit);
  res.json({ total, returned: rows.length, events: rows, summary });
});

app.get('/api/filters', auth, (req, res) => {
  if (db) {
    try {
      const events = db.prepare(`SELECT DISTINCT event FROM events WHERE event != '' ORDER BY event ASC LIMIT 1000`).all().map(r => r.event);
      const users = db.prepare(`SELECT DISTINCT user FROM events WHERE user != '' ORDER BY user ASC LIMIT 1000`).all().map(r => r.user);
      const ips = db.prepare(`SELECT DISTINCT ip FROM events WHERE ip != '' ORDER BY ip ASC LIMIT 1000`).all().map(r => r.ip);
      return res.json({ events, users, ips });
    } catch (err) {
      log('error', `API /filters (db): ${err.message}`);
      return res.status(500).json({ error: 'DB query failed' });
    }
  }
  const uniq = arr => [...new Set(arr.filter(Boolean))].sort();
  res.json({
    events: uniq(events.map(e => e.event)),
    users: uniq(events.map(e => e.user)),
    ips: uniq(events.map(e => e.ip)),
  });
});

app.get('/api/distinct-values', auth, (req, res) => {
  const field = String(req.query.field || '');
  const allowedFields = { ip: 'ip', user: 'user' };
  const col = allowedFields[field];
  if (!col) return res.status(400).json({ error: 'field must be ip or user' });

  const { user = '', event = '', ip = '', source = '', from = '', to = '', q = '' } = req.query;
  const limit = Math.max(1, Math.min(Number(req.query.limit || 10000), 50000));

  if (db) {
    try {
      const where = [];
      const params = {};
      if (user) { where.push('user = @user'); params.user = user; }
      if (event) { where.push('event = @event'); params.event = event; }
      if (ip) { where.push('ip = @ip'); params.ip = ip; }
      if (source) { where.push('source = @source'); params.source = source; }
      const fromTs = from ? parseTimestampMs(from) : 0;
      const toTs = to ? parseTimestampMs(to) : 0;
      if (from && fromTs) { where.push('event_ts(time, receivedAt) >= @fromTs'); params.fromTs = fromTs; }
      if (to && toTs) { where.push('event_ts(time, receivedAt) <= @toTs'); params.toTs = toTs; }
      if (q) {
        where.push(`(
          icontains(journal, @q) = 1 OR
          icontains(time, @q) = 1 OR
          icontains(ip, @q) = 1 OR
          icontains(user, @q) = 1 OR
          icontains(event, @q) = 1 OR
          icontains(filetype, @q) = 1 OR
          icontains(size, @q) = 1 OR
          icontains(path, @q) = 1 OR
          icontains(source, @q) = 1 OR
          icontains(receivedAt, @q) = 1 OR
          icontains(raw, @q) = 1
        )`);
        params.q = String(q);
      }
      where.push(`${col} != ''`);
      const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
      const rows = db.prepare(`
        SELECT DISTINCT ${col} AS value
        FROM events
        ${whereSql}
        ORDER BY ${col} ASC
        LIMIT @limit
      `).all({ ...params, limit });
      return res.json({ field: col, values: rows.map(r => r.value) });
    } catch (err) {
      log('error', `API /distinct-values (db): ${err.message}`);
      return res.status(500).json({ error: 'DB query failed' });
    }
  }

  let r = [...events];
  if (user)  r = r.filter(e => e.user === user);
  if (event) r = r.filter(e => e.event === event);
  if (ip)    r = r.filter(e => e.ip === ip);
  if (source) r = r.filter(e => e.source === source);
  if (from) {
    const fromTs = parseTimestampMs(from);
    if (fromTs) r = r.filter(e => eventTimestampMs(e.time, e.receivedAt) >= fromTs);
  }
  if (to) {
    const toTs = parseTimestampMs(to);
    if (toTs) r = r.filter(e => eventTimestampMs(e.time, e.receivedAt) <= toTs);
  }
  if (q)     r = r.filter(e => includesNormalized(JSON.stringify(e), q));
  const values = [...new Set(r.map(e => String(e[col] || '').trim()).filter(Boolean))].sort().slice(0, limit);
  res.json({ field: col, values });
});

app.post('/api/upload-csv', auth, express.text({ type: '*/*', limit: '50mb' }), (req, res) => {
  try {
    const parsed = parseCSV(req.body);
    let imported = 0;
    parsed.forEach(e => {
      e.source = 'upload';
      if (addEvent(e)) {
        stats.csv++;
        imported++;
      }
    });
    log('info', `CSV upload: ${imported} из ${parsed.length} событий (дубли пропущены)`);
    res.json({ ok: true, imported, totalParsed: parsed.length });
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
  const mPath = () => {
    const patterns = [
      /(?:path|file|object|filename|folder|directory|dir)[=:\s]+"([^"]+)"/i,
      /(?:path|file|object|filename|folder|directory|dir)[=:\s]+'([^']+)'/i,
      /(?:path|file|object|filename|folder|directory|dir)[=:\s]+([^\s,;]+)/i,
      /(?:файл|путь|объект|папка)[=:\s]+"([^"]+)"/i,
      /(?:файл|путь|объект|папка)[=:\s]+([^\s,;]+)/i,
    ];
    for (const re of patterns) {
      const r = str.match(re);
      if (r && r[1]) return r[1];
    }
    return '';
  };
  const ev = { journal: 'Syslog', raw: str, source: 'syslog' };

  ev.time     = m(/(\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2})/)
             || m(/^<\d+>(\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2})/)
             || new Date().toISOString().slice(0, 19).replace('T', ' ');
  ev.user     = m(/user[=:\s]+([^\s,;"]+)/i)              || 'system';
  ev.ip       = m(/(?:ip|from)[=:\s]+([\d.]+)/i)          || '';
  ev.path     = mPath();
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
    if (addEvent(ev)) {
      stats.syslog++;
      log('debug', `Syslog: ${rinfo.address} → ${ev.event} ${ev.user} ${ev.path}`);
    }
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
      let inserted = 0;
      parsed.forEach(e => {
        if (addEvent(e)) {
          stats.csv++;
          inserted++;
        }
      });
      log('info', `Poll: загружено ${inserted} из ${parsed.length} событий (дубли пропущены)`);
    } else {
      const newL = lines.slice(lastLineCount);
      if (newL.length > 0) {
        const parsed = parseCSV([lines[0], ...newL].join('\n'));
        let inserted = 0;
        parsed.forEach(e => {
          if (addEvent(e)) {
            stats.csv++;
            inserted++;
          }
        });
        log('info', `Poll: +${inserted} из ${parsed.length} новых событий`);
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
  server.close(() => {
    if (db) {
      try { db.close(); } catch (_) {}
    }
    log('info', 'Сервер остановлен');
    process.exit(0);
  });
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
