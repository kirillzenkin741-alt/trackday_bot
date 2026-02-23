# Trackbot on Debian (Runbook)

## 1. System packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip sqlite3
```

## 2. Project setup

```bash
cd /opt/trackbot
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Config

Create `.env` in project root:

```env
BOT_TOKEN=...
GROUP_ID=...
ADMIN_ID=...
```

Place Google service account file as `credentials.json` in project root.

## 4. Smoke checks

```bash
. .venv/bin/activate
python -c "import aiogram, aiohttp, gspread; print('imports: ok')"
python -c "import pytesseract, PIL; print('ocr fallback imports: ok')"  # optional
python -m py_compile bot.py
python scripts/healthcheck.py
python bot.py
```

Expected:
- Bot starts without traceback.
- DB schema/migrations are applied.
- Scheduler starts.
- Bot responds to `/start`.

Stop manual run with `Ctrl+C`.

## 5. systemd service

Create `/etc/systemd/system/trackbot.service`:

```ini
[Unit]
Description=Trackbot Telegram Bot
After=network.target

[Service]
Type=simple
User=trackbot
WorkingDirectory=/opt/trackbot
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/opt/trackbot/.env
ExecStart=/opt/trackbot/.venv/bin/python /opt/trackbot/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and check:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now trackbot
sudo systemctl status trackbot
journalctl -u trackbot -f
```

## 6. Operational checklist

- `/startcollection` creates current week session.
- `/startvoting` posts tracks, nomination blocks (if active), and main vote block.
- Users can revote both nomination/main before finish.
- `/finishvoting` posts results and updates leaderboard.
- `/history` shows weekly winners from SQLite.

## 7. DB backup (daily cron)

```bash
cd /opt/trackbot
. .venv/bin/activate
python scripts/backup_db.py
```

Example cron (daily at 03:40):

```bash
40 3 * * * cd /opt/trackbot && . .venv/bin/activate && python scripts/backup_db.py >> /var/log/trackbot-backup.log 2>&1
```
