from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _ensure_repo_root_on_sys_path() -> None:
    if __package__ not in {None, ""}:
        return

    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)


_ensure_repo_root_on_sys_path()

from src.eastmoney_author_tracker.ocr import DeferredOcrProvider, OptionalTesseractOcrProvider
from src.eastmoney_author_tracker.service import enrich_pending_author_images, sync_author_activity
from src.eastmoney_author_tracker.store import get_engine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Eastmoney author activity into the local tracker store.")
    parser.add_argument("--author-uid", required=True, help="Eastmoney author UID, e.g. 4348595203199492")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages to fetch per run")
    parser.add_argument("--page-size", type=int, default=20, help="Page size for Eastmoney API requests")
    parser.add_argument(
        "--unchanged-post-stop-count",
        type=int,
        default=10,
        help="Stop paging after this many consecutive known unchanged posts",
    )
    parser.add_argument(
        "--reply-cutoff-date",
        default=None,
        help="Only backfill per-post author replies for posts whose activity time is on/after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--use-tesseract",
        action="store_true",
        help="Enable local Tesseract OCR for --ocr-inline or --enrich-pending-ocr",
    )
    parser.add_argument("--ocr-inline", action="store_true", help="Run OCR during the main sync instead of deferring image OCR")
    parser.add_argument("--enrich-pending-ocr", action="store_true", help="Process pending OCR images after sync, or by itself with --skip-sync")
    parser.add_argument("--skip-sync", action="store_true", help="Skip the main sync and only run the pending OCR enrichment flow")
    parser.add_argument("--ocr-limit", type=int, default=50, help="Maximum number of pending OCR images to process per run")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.ocr_inline and not args.use_tesseract:
        raise SystemExit("--ocr-inline requires --use-tesseract")
    if args.enrich_pending_ocr and not args.use_tesseract:
        raise SystemExit("--enrich-pending-ocr requires --use-tesseract")
    if args.skip_sync and not args.enrich_pending_ocr:
        raise SystemExit("--skip-sync requires --enrich-pending-ocr")

    engine = get_engine()
    tesseract_provider = OptionalTesseractOcrProvider() if args.use_tesseract else None

    output: dict[str, object] = {}
    if not args.skip_sync:
        sync_ocr_provider = tesseract_provider if args.ocr_inline else DeferredOcrProvider()
        output["sync"] = sync_author_activity(
            engine,
            args.author_uid,
            max_pages=args.max_pages,
            page_size=args.page_size,
            ocr_provider=sync_ocr_provider,
            unchanged_post_stop_count=args.unchanged_post_stop_count,
            reply_cutoff_date=args.reply_cutoff_date,
        )

    if args.enrich_pending_ocr:
        output["ocr_enrichment"] = enrich_pending_author_images(
            engine,
            args.author_uid,
            ocr_provider=tesseract_provider,
            limit=args.ocr_limit,
        )

    rendered_output: object = next(iter(output.values())) if len(output) == 1 else output
    print(json.dumps(rendered_output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
