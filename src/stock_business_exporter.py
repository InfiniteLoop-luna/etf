from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.sync_tushare_security_data import build_active_stock_sql_clause, get_engine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output" / "stock_business"
SHEET_NAME = "个股业务范围"
EXPORT_COLUMNS = ["股票代码", "当前主要业务", "当前产品及业务范围"]


def build_stock_business_sql(active_only: bool = True) -> str:
    where_clause = ""
    if active_only:
        where_clause = f"""
        WHERE {build_active_stock_sql_clause("basic")}
          AND COALESCE(basic.delist_date::text, '') = ''
        """

    return f"""
        SELECT
            basic.ts_code,
            company.main_business,
            company.business_scope
        FROM vw_ts_stock_basic AS basic
        LEFT JOIN vw_ts_stock_company AS company
          ON basic.ts_code = company.ts_code
        {where_clause}
        ORDER BY basic.ts_code
    """


def format_stock_business_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    export_df = pd.DataFrame(
        {
            "股票代码": _clean_text_column(raw_df, "ts_code"),
            "当前主要业务": _clean_text_column(raw_df, "main_business"),
            "当前产品及业务范围": _clean_text_column(raw_df, "business_scope"),
        }
    )
    return export_df[EXPORT_COLUMNS]


def fetch_stock_business_dataframe(engine=None, active_only: bool = True) -> pd.DataFrame:
    if engine is None:
        engine = get_engine()

    raw_df = pd.read_sql(text(build_stock_business_sql(active_only=active_only)), engine)
    return format_stock_business_dataframe(raw_df)


def export_stock_business_excel(
    df: pd.DataFrame,
    output_dir: str | Path | None = None,
    run_date: date | None = None,
) -> Path:
    output_path = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    output_path.mkdir(parents=True, exist_ok=True)

    effective_date = run_date or date.today()
    file_path = output_path / f"个股业务范围_{effective_date.strftime('%Y%m%d')}.xlsx"

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=SHEET_NAME, index=False)
        worksheet = writer.sheets[SHEET_NAME]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions
        worksheet.column_dimensions["A"].width = 14
        worksheet.column_dimensions["B"].width = 44
        worksheet.column_dimensions["C"].width = 80

    return file_path


def export_stock_business_from_database(
    engine=None,
    output_dir: str | Path | None = None,
    active_only: bool = True,
    run_date: date | None = None,
) -> tuple[Path, int]:
    df = fetch_stock_business_dataframe(engine=engine, active_only=active_only)
    file_path = export_stock_business_excel(df, output_dir=output_dir, run_date=run_date)
    return file_path, len(df)


def _clean_text_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[column].where(pd.notna(df[column]), "").astype(str).str.strip()
