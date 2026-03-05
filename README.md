# VM Log Server — Интеграция с Synology NAS

## Быстрый старт

```bash
npm install
node server.js
```

Откройте `client.html` в браузере и нажмите **Подключить** (ws://localhost:3000).

---

## Переменные окружения

| Переменная      | По умолчанию        | Описание                          |
|-----------------|---------------------|-----------------------------------|
| `NAS_CSV_PATH`  | `./nas_log.csv`     | Путь к CSV-файлу от Synology      |
| `SYSLOG_PORT`   | `514`               | UDP порт для syslog               |
| `HTTP_PORT`     | `3000`              | HTTP/WebSocket порт               |

Пример:
```bash
NAS_CSV_PATH=/mnt/nas/logs/audit.csv HTTP_PORT=8080 node server.js
```

---

## Настройка Synology DSM

### 1. Включить SMB-аудит
**Control Panel → File Services → SMB → Advanced**  
→ Enable transfer log  
→ Выбрать типы: Create, Delete, Read, Write, Rename, Move

### 2. Syslog (real-time)
**Log Center → Log Sending → Add**
- Destination: IP-адрес машины с сервером
- Port: 514
- Protocol: UDP
- Format: IETF (RFC 5424)

> Если нет прав на порт 514 (нужен root), используйте порт 5514 и настройте перенаправление:
> ```bash
> sudo iptables -t nat -A PREROUTING -p udp --dport 514 -j REDIRECT --to-port 5514
> ```
> Затем в `server.js` измените `syslogPort: 5514`

### 3. CSV Polling
Synology должна экспортировать CSV в общую папку:

**DSM Task Scheduler** → Add → User-defined script (каждые 5 минут):
```bash
# Пример скрипта экспорта (путь зависит от DSM версии)
# Log Center → Export → CSV → сохранить в /volume1/logs/audit.csv
```

Примонтируйте папку на машине с сервером:
```bash
# Linux:
sudo mount -t cifs //192.168.1.100/logs /mnt/nas/logs -o username=admin,password=XXX

# Windows:
net use Z: \\192.168.1.100\logs /user:admin PASSWORD
```

Укажите путь:
```bash
NAS_CSV_PATH=/mnt/nas/logs/audit.csv node server.js
```

---

## API

| Метод | URL               | Описание                         |
|-------|-------------------|----------------------------------|
| GET   | `/api/status`     | Статус сервера и статистика      |
| GET   | `/api/events`     | Список событий с фильтрами       |
| POST  | `/api/upload-csv` | Загрузить CSV (text/plain body)  |

### Параметры `/api/events`
- `user` — фильтр по пользователю
- `event` — фильтр по событию
- `ip` — фильтр по IP
- `q` — текстовый поиск
- `from` / `to` — диапазон дат
- `limit` / `offset` — пагинация

---

## Архитектура

```
Synology NAS
├── SMB Audit → UDP Syslog ──────────→ server.js :514 ─┐
├── Log Center → CSV export                              ├→ WebSocket → client.html
│   └── /shared/logs/audit.csv ──→ Poll каждые 30с ────┤
└── Ручной экспорт CSV                                   │
    └── HTTP POST /api/upload-csv ──────────────────────┘
```
