from __future__ import annotations

import argparse
import re
import subprocess
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


def _to_repo_relpath(path: Path, project_root: Path) -> str:
    return str(path.relative_to(project_root)).replace("\\", "/")


def _classify_git_tracking(project_root: Path, paths: list[Path]) -> dict[str, str]:
    relpaths = [_to_repo_relpath(path, project_root) for path in paths]
    tracked: set[str] = set()
    if relpaths:
        try:
            result = subprocess.run(
                ["git", "-C", str(project_root), "ls-files", "--", *relpaths],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            tracked = {line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()}
        except Exception:
            tracked = set()
    return {relpath: ("tracked" if relpath in tracked else "untracked") for relpath in relpaths}


def cleanup_archives(keep_days: int, dry_run: bool = False) -> dict:
    expired = find_expired_archives(RECOMMENDATIONS_DIR, keep_days=keep_days)
    tracking = _classify_git_tracking(PROJECT_ROOT, expired)
    removed: list[str] = []
    removed_details: list[dict[str, str]] = []

    for path in expired:
        relpath = _to_repo_relpath(path, PROJECT_ROOT)
        tracking_state = tracking.get(relpath, "unknown")
        removed.append(relpath)
        removed_details.append({"path": relpath, "git_tracking": tracking_state})
        if not dry_run:
            path.unlink(missing_ok=True)

    tracked_count = sum(1 for item in removed_details if item["git_tracking"] == "tracked")
    untracked_count = sum(1 for item in removed_details if item["git_tracking"] == "untracked")

    return {
        "keep_days": int(keep_days),
        "dry_run": bool(dry_run),
        "removed_count": len(removed),
        "tracked_removed_count": tracked_count,
        "untracked_removed_count": untracked_count,
        "removed": removed,
        "removed_details": removed_details,
        "summary_message": (
            f"cleanup {'would remove' if dry_run else 'removed'} {len(removed)} archive(s) "
            f"(tracked={tracked_count}, untracked={untracked_count})"
        ),
    }


def main() -> int:
    args = parse_args()
    summary = cleanup_archives(keep_days=args.keep_days, dry_run=args.dry_run)
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
