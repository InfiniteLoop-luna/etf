from __future__ import annotations

import json
import sys

from sqlalchemy import text

from src.ml_stock_dataset import get_engine


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: terminate_backend.py <pid> [<pid> ...]")

    pids = [int(arg) for arg in sys.argv[1:]]
    engine = get_engine()
    results = []
    with engine.begin() as conn:
        for pid in pids:
            terminated = conn.execute(
                text("select pg_terminate_backend(:pid)"),
                {"pid": pid},
            ).scalar()
            results.append({"pid": pid, "terminated": bool(terminated)})

    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
