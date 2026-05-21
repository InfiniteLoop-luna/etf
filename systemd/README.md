# systemd assets

This directory contains repo-managed systemd/runtime assets for the ETF VPS.

## Files

- `etf-data-update.service`
- `etf-data-update.timer`
- `etf-daily-8pm.service`
- `etf-daily-8pm.timer`
- `etf-daily-8pm.sh`

## Current production mapping

Production currently uses:

- `/etc/systemd/system/etf-daily-8pm.service`
- `/etc/systemd/system/etf-daily-8pm.timer`
- `/usr/local/bin/etf-daily-8pm.sh`

## Sync commands on VPS

```bash
install -m 755 systemd/etf-daily-8pm.sh /usr/local/bin/etf-daily-8pm.sh
install -m 644 systemd/etf-daily-8pm.service /etc/systemd/system/etf-daily-8pm.service
install -m 644 systemd/etf-daily-8pm.timer /etc/systemd/system/etf-daily-8pm.timer
systemctl daemon-reload
systemctl restart etf-daily-8pm.timer
systemctl status etf-daily-8pm.timer --no-pager
```

## Notes

- `etf-daily-8pm.sh` must not mutate tracked repo files at runtime.
- Nightly behavior overrides should flow through environment variables consumed by `scripts/etf-data-update.sh`.
