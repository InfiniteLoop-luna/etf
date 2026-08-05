from datetime import datetime
from pathlib import Path

from scripts.cleanup_trend_recommendation_archives import cleanup_archives, find_expired_archives


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n", encoding="utf-8")


def test_find_expired_archives_keeps_latest_and_recent(tmp_path: Path):
    rec_dir = tmp_path / "recommendations"
    _touch(rec_dir / "latest_trend_recommendations.json")
    _touch(rec_dir / "2026-08-05_trend_recommendations.json")
    _touch(rec_dir / "2026-07-05_trend_recommendations.json")
    _touch(rec_dir / "README.json")

    expired = find_expired_archives(
        rec_dir,
        keep_days=30,
        now=datetime(2026, 8, 5, 12, 0, 0),
    )

    assert [path.name for path in expired] == ["2026-07-05_trend_recommendations.json"]


def test_cleanup_archives_dry_run_does_not_delete_files(tmp_path: Path, monkeypatch):
    project_root = tmp_path
    rec_dir = project_root / "data" / "recommendations"
    _touch(rec_dir / "2026-07-01_trend_recommendations.json")
    _touch(rec_dir / "latest_trend_recommendations.json")

    monkeypatch.setattr("scripts.cleanup_trend_recommendation_archives.PROJECT_ROOT", project_root)
    monkeypatch.setattr("scripts.cleanup_trend_recommendation_archives.RECOMMENDATIONS_DIR", rec_dir)
    monkeypatch.setattr(
        "scripts.cleanup_trend_recommendation_archives.datetime",
        type("FakeDateTime", (), {"now": staticmethod(lambda tz=None: datetime(2026, 8, 5, 12, 0, 0, tzinfo=tz))}),
    )

    monkeypatch.setattr(
        "scripts.cleanup_trend_recommendation_archives._classify_git_tracking",
        lambda project_root, paths: {"data/recommendations/2026-07-01_trend_recommendations.json": "untracked"},
    )

    summary = cleanup_archives(keep_days=30, dry_run=True)

    assert summary["removed_count"] == 1
    assert summary["tracked_removed_count"] == 0
    assert summary["untracked_removed_count"] == 1
    assert "tracked=0, untracked=1" in summary["summary_message"]
    assert summary["removed_details"] == [
        {
            "path": "data/recommendations/2026-07-01_trend_recommendations.json",
            "git_tracking": "untracked",
        }
    ]
    assert (rec_dir / "2026-07-01_trend_recommendations.json").exists()


def test_cleanup_archives_deletes_expired_files(tmp_path: Path, monkeypatch):
    project_root = tmp_path
    rec_dir = project_root / "data" / "recommendations"
    old_file = rec_dir / "2026-07-01_trend_recommendations.json"
    recent_file = rec_dir / "2026-07-15_trend_recommendations.json"
    latest_file = rec_dir / "latest_trend_recommendations.json"
    _touch(old_file)
    _touch(recent_file)
    _touch(latest_file)

    monkeypatch.setattr("scripts.cleanup_trend_recommendation_archives.PROJECT_ROOT", project_root)
    monkeypatch.setattr("scripts.cleanup_trend_recommendation_archives.RECOMMENDATIONS_DIR", rec_dir)
    monkeypatch.setattr(
        "scripts.cleanup_trend_recommendation_archives.datetime",
        type("FakeDateTime", (), {"now": staticmethod(lambda tz=None: datetime(2026, 8, 5, 12, 0, 0, tzinfo=tz))}),
    )

    monkeypatch.setattr(
        "scripts.cleanup_trend_recommendation_archives._classify_git_tracking",
        lambda project_root, paths: {
            "data/recommendations/2026-07-01_trend_recommendations.json": "tracked",
        },
    )

    summary = cleanup_archives(keep_days=30, dry_run=False)

    assert summary["removed"] == ["data/recommendations/2026-07-01_trend_recommendations.json"]
    assert summary["tracked_removed_count"] == 1
    assert summary["untracked_removed_count"] == 0
    assert summary["removed_details"] == [
        {
            "path": "data/recommendations/2026-07-01_trend_recommendations.json",
            "git_tracking": "tracked",
        }
    ]
    assert not old_file.exists()
    assert recent_file.exists()
    assert latest_file.exists()
