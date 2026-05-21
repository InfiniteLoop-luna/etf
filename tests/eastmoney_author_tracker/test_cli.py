import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.sync_eastmoney_author as cli


class SyncAuthorCliTests(unittest.TestCase):
    def test_ensure_repo_root_on_sys_path_adds_repo_root_for_direct_script_mode(self):
        repo_root = str(Path(cli.__file__).resolve().parents[1])
        trimmed_path = [value for value in sys.path if value != repo_root]

        with patch.object(cli, "__package__", ""):
            with patch.object(sys, "path", trimmed_path.copy()):
                cli._ensure_repo_root_on_sys_path()

                self.assertEqual(sys.path[0], repo_root)

    def test_main_rejects_inline_ocr_without_tesseract(self):
        with patch.object(sys, "argv", ["sync_eastmoney_author.py", "--author-uid", "4348595203199492", "--ocr-inline"]):
            with self.assertRaises(SystemExit) as exc_context:
                cli.main()

        self.assertEqual(str(exc_context.exception), "--ocr-inline requires --use-tesseract")

    def test_main_rejects_pending_enrichment_without_tesseract(self):
        with patch.object(
            sys,
            "argv",
            ["sync_eastmoney_author.py", "--author-uid", "4348595203199492", "--enrich-pending-ocr"],
        ):
            with self.assertRaises(SystemExit) as exc_context:
                cli.main()

        self.assertEqual(str(exc_context.exception), "--enrich-pending-ocr requires --use-tesseract")

    @patch("scripts.sync_eastmoney_author.enrich_pending_author_images", return_value={"processed_image_count": 5})
    @patch("scripts.sync_eastmoney_author.sync_author_activity", return_value={"cycle_count": 2})
    @patch("scripts.sync_eastmoney_author.OptionalTesseractOcrProvider")
    @patch("scripts.sync_eastmoney_author.get_engine", return_value="engine")
    def test_main_runs_sync_and_pending_ocr_with_expected_arguments(
        self,
        _mock_get_engine,
        mock_tesseract_provider,
        mock_sync_author_activity,
        mock_enrich_pending_author_images,
    ):
        provider = object()
        mock_tesseract_provider.return_value = provider

        with patch.object(
            sys,
            "argv",
            [
                "sync_eastmoney_author.py",
                "--author-uid",
                "4348595203199492",
                "--max-pages",
                "7",
                "--page-size",
                "30",
                "--unchanged-post-stop-count",
                "2",
                "--reply-cutoff-date",
                "2026-04-01",
                "--ocr-limit",
                "60",
                "--use-tesseract",
                "--ocr-inline",
                "--enrich-pending-ocr",
            ],
        ):
            result = cli.main()

        self.assertEqual(result, 0)
        mock_sync_author_activity.assert_called_once_with(
            "engine",
            "4348595203199492",
            max_pages=7,
            page_size=30,
            ocr_provider=provider,
            unchanged_post_stop_count=2,
            reply_cutoff_date="2026-04-01",
        )
        mock_enrich_pending_author_images.assert_called_once_with(
            "engine",
            "4348595203199492",
            ocr_provider=provider,
            limit=60,
        )

    def test_nightly_update_script_passes_reply_cutoff_date(self):
        script_source = Path("scripts/etf-data-update.sh").read_text(encoding="utf-8")

        self.assertIn("--reply-cutoff-date 2026-04-01", script_source)


if __name__ == "__main__":
    unittest.main()
