# systemd assets

This directory contains repo-managed systemd/runtime assets for the ETF VPS.

## Files

- `etf-data-update.service`
- `etf-data-update.timer`
- `etf-daily-8pm.service`
- `etf-daily-8pm.timer`
- `etf-daily-8pm.sh`
- `etf-stock-business-daily.service`
- `etf-stock-business-daily.timer`
- `etf-stock-business-daily.sh`
- `etf-fund-estimate-snapshot.service`
- `etf-fund-estimate-snapshot.timer`

## Current production mapping

Production currently uses:

- `/etc/systemd/system/etf-daily-8pm.service`
- `/etc/systemd/system/etf-daily-8pm.timer`
- `/usr/local/bin/etf-daily-8pm.sh`
- `/etc/systemd/system/etf-stock-business-daily.service`
- `/etc/systemd/system/etf-stock-business-daily.timer`
- `/usr/local/bin/etf-stock-business-daily.sh`

## Sync commands on VPS

```bash
install -m 755 systemd/etf-daily-8pm.sh /usr/local/bin/etf-daily-8pm.sh
install -m 644 systemd/etf-daily-8pm.service /etc/systemd/system/etf-daily-8pm.service
install -m 644 systemd/etf-daily-8pm.timer /etc/systemd/system/etf-daily-8pm.timer
systemctl daemon-reload
systemctl restart etf-daily-8pm.timer
systemctl status etf-daily-8pm.timer --no-pager
```

## Stock business daily Excel backup

This timer exports `output/stock_business/个股业务范围_YYYYMMDD.xlsx`
from the project PostgreSQL database every day at 21:00 Asia/Shanghai.
If `STOCK_BUSINESS_BACKUP_GIT_URL` is configured, the same run also pushes
the exported Excel files to a separate GitHub backup repository.

Recommended `/opt/etf-app/.env` settings:

```bash
STOCK_BUSINESS_BACKUP_GIT_URL=git@github.com:YOUR_ACCOUNT/YOUR_BACKUP_REPO.git
STOCK_BUSINESS_BACKUP_DIR=/opt/etf-stock-business-backup
STOCK_BUSINESS_BACKUP_BRANCH=main
STOCK_BUSINESS_BACKUP_SUBDIR=stock_business
```

The VPS user running the service must be able to push to that GitHub repo
non-interactively, typically via an SSH deploy key with write access.

```bash
install -m 755 systemd/etf-stock-business-daily.sh /usr/local/bin/etf-stock-business-daily.sh
install -m 644 systemd/etf-stock-business-daily.service /etc/systemd/system/etf-stock-business-daily.service
install -m 644 systemd/etf-stock-business-daily.timer /etc/systemd/system/etf-stock-business-daily.timer
systemctl daemon-reload
systemctl enable --now etf-stock-business-daily.timer
systemctl list-timers etf-stock-business-daily.timer --no-pager
```

## Fund watchlist 15:00 estimate snapshots

The fund estimate timer runs at 15:05 Asia/Shanghai on weekdays. It saves the
15:00 Tencent quote-based estimate for the union of all users' watchlist funds.
The watchlist page only compares a saved estimate with an actual NAV return when
their dates are identical.
The existing 20:00 data update also runs the same command as a fallback; an
already saved quote closer to 15:00 is retained instead of being overwritten.

```bash
install -m 644 systemd/etf-fund-estimate-snapshot.service /etc/systemd/system/etf-fund-estimate-snapshot.service
install -m 644 systemd/etf-fund-estimate-snapshot.timer /etc/systemd/system/etf-fund-estimate-snapshot.timer
systemctl daemon-reload
systemctl enable --now etf-fund-estimate-snapshot.timer
systemctl list-timers etf-fund-estimate-snapshot.timer --no-pager
```

Manual one-shot validation after 15:00 Asia/Shanghai:

```bash
systemctl start etf-fund-estimate-snapshot.service
journalctl -u etf-fund-estimate-snapshot.service -n 80 --no-pager
```

Manual one-shot validation:

```bash
systemctl start etf-stock-business-daily.service
journalctl -u etf-stock-business-daily.service -n 80 --no-pager
ls -lh /opt/etf-app/output/stock_business/
git -C /opt/etf-stock-business-backup log --oneline -5
```

## Notes

- `etf-daily-8pm.sh` must not mutate tracked repo files at runtime.
- Nightly behavior overrides should flow through environment variables consumed by `scripts/etf-data-update.sh`.
- `etf-stock-business-daily.sh` reads `/opt/etf-app/.env` and uses `/opt/etf-app/.venv/bin/python`.
- `etf-fund-estimate-snapshot.service` reads `/opt/etf-app/.env` and uses the project virtualenv directly.
- GitHub backup should use a private backup repository unless the exported business data is intended to be public.
