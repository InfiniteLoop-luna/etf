from __future__ import annotations

import argparse
import json

from src.eastmoney_author_tracker.ocr import NullOcrProvider, OptionalTesseractOcrProvider
from src.eastmoney_author_tracker.service import sync_author_activity
from src.eastmoney_author_tracker.store import get_engine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Eastmoney author activity into the local tracker store.")
    parser.add_argument("--author-uid", required=True, help="Eastmoney author UID, e.g. 4348595203199492")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages to fetch per run")
    parser.add_argument("--page-size", type=int, default=20, help="Page size for Eastmoney API requests")
    parser.add_argument("--use-tesseract", action="store_true", help="Enable optional local Tesseract OCR provider")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    engine = get_engine()
    ocr_provider = OptionalTesseractOcrProvider() if args.use_tesseract else NullOcrProvider()
    summary = sync_author_activity(
        engine,
        args.author_uid,
        max_pages=args.max_pages,
        page_size=args.page_size,
        ocr_provider=ocr_provider,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
