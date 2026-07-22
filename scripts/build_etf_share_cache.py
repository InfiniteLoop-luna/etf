import argparse
import gzip
import hashlib
import json
import os
import pickle
from datetime import datetime, timezone

from src.data_loader import load_etf_data


def _sha256_of_file(file_path: str) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--excel", default="主要ETF基金份额变动情况.xlsx")
    parser.add_argument("--output", default=os.path.join("data", "etf_share_cache.pkl.gz"))
    parser.add_argument("--meta", default=os.path.join("data", "etf_share_cache.meta.json"))
    parser.add_argument("--skip-if-fresh", action="store_true")
    args = parser.parse_args()

    excel_path = args.excel
    if not os.path.exists(excel_path):
        raise FileNotFoundError(excel_path)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.meta) or ".", exist_ok=True)

    file_stat = os.stat(excel_path)
    file_sha256 = _sha256_of_file(excel_path)

    if args.skip_if_fresh and os.path.exists(args.meta):
        try:
            with open(args.meta, "r", encoding="utf-8") as f:
                existing_meta = json.load(f)
            if existing_meta.get("excel_sha256") == file_sha256 and os.path.exists(args.output):
                print(json.dumps(existing_meta, ensure_ascii=False))
                return 0
        except Exception:
            pass

    df = load_etf_data(excel_path)

    with gzip.open(args.output, "wb") as f:
        pickle.dump(df, f, protocol=pickle.HIGHEST_PROTOCOL)

    meta = {
        "excel_path": os.path.basename(excel_path),
        "excel_size": int(file_stat.st_size),
        "excel_sha256": file_sha256,
        "row_count": int(len(df)),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(args.meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
