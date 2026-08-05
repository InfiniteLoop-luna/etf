from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECOMMENDATIONS_DIR = PROJECT_ROOT / "data" / "recommendations"
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
ARCHIVE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})_trend_recommendations\.json$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean old trend recommendation archive files.")
    parser.add_argument("--keep-days", type=int, default=30, help="Keep archives within the most recent N days (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Only print files that would be removed")
    return parser.parse_args()


def find_expired_archives(base_dir: Path, keep_days: int, now: datetime | None = None) -> list[Path]:
    now = now or datetime.now(BEIJING_TZ)
    cutoff_date = (now.date() - timedelta(days=max(int(keep_days), 0)))
    expired: list[Path] = []

    if not base_dir.exists():
        return expired

    for path in sorted(base_dir.glob("*_trend_recommendations.json")):
        if path.name == "latest_trend_recommendations.json":
            continue
        match = ARCHIVE_PATTERN.match(path.name)
        if not match:
            continue
        try:
            archive_date = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if archive_date < cutoff_date:
            expired.append(path)
    return expired


def cleanup_archives(keep_days: int, dry_run: bool = False) -> dict:
    expired = find_expired_archives(RECOMMENDATIONS_DIR, keep_days=keep_days)
    removed: list[str] = []

    for path in expired:
        removed.append(str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"))
        if not dry_run:
            path.unlink(missing_ok=True)

    return {
        "keep_days": int(keep_days),
        "dry_run": bool(dry_run),
        "removed_count": len(removed),
        "removed": removed,
    }


def main() -> int:
    args = parse_args()
    summary = cleanup_archives(keep_days=args.keep_days, dry_run=args.dry_run)
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
