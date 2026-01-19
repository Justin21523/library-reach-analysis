# Ops (Always-On)

## Docker compose

- Start: `docker compose up -d --build`
- Stop: `docker compose down`
- Logs: `docker compose logs -f worker` / `docker compose logs -f api`
- Health: `curl http://127.0.0.1:${LR_HOST_PORT:-8001}/health`

The worker writes:
- `data/raw/tdx/ingestion_status.json` (latest ingestion state + rate-limit signals)
- `data/raw/tdx/stops.meta.json` (ingestion provenance)

## Backup (lightweight)

This repo includes a small backup script that captures “catalogs + latest summaries + run_meta + QA + ingestion meta”.

- Run once: `bash scripts/backup.sh`
- Keep only last 7 backups: `KEEP_N=7 bash scripts/backup.sh`

Backups are written to `./backups/` by default. Raw `stops.csv` is intentionally excluded (large, rebuildable).

## Autostart on boot (systemd example)

If you’re on Linux with systemd, you can run docker compose at boot.

1) Create a unit file:

`/etc/systemd/system/libraryreach.service`

```
[Unit]
Description=LibraryReach (docker compose)
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/home/justin/web-projects/library-reach-analysis
ExecStart=/usr/bin/docker compose up -d --build
ExecStop=/usr/bin/docker compose down
RemainAfterExit=yes
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

2) Enable and start:

`sudo systemctl daemon-reload`

`sudo systemctl enable --now libraryreach`

Notes:
- Make sure your Docker service is configured to start at boot.
- Update `WorkingDirectory` to your repo path.
