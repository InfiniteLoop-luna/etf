from pathlib import Path

from scripts.cleanup_trend_recommendation_archives import _classify_git_tracking


def test_classify_git_tracking_marks_missing_files_as_untracked(tmp_path: Path):
    project_root = tmp_path / "repo"
    project_root.mkdir(parents=True, exist_ok=True)

    result = _classify_git_tracking(
        project_root,
        [project_root / "data" / "recommendations" / "2026-07-01_trend_recommendations.json"],
    )

    assert result == {
        "data/recommendations/2026-07-01_trend_recommendations.json": "untracked"
    }
