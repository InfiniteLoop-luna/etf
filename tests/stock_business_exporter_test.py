import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

from scripts.export_stock_business_to_excel import main, parse_args
from src.stock_business_exporter import (
    EXPORT_COLUMNS,
    build_stock_business_sql,
    export_stock_business_excel,
    fetch_stock_business_dataframe,
)


class StockBusinessExporterTests(unittest.TestCase):
    def test_fetch_stock_business_dataframe_reads_database_views_and_formats_columns(self):
        raw = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "main_business": "商业银行业务",
                    "business_scope": "吸收公众存款；发放贷款。",
                },
                {
                    "ts_code": "600000.SH",
                    "main_business": None,
                    "business_scope": None,
                },
            ]
        )
        calls = []

        def fake_read_sql(sql, engine):
            calls.append((str(sql), engine))
            return raw

        fake_engine = object()
        with patch("src.stock_business_exporter.pd.read_sql", side_effect=fake_read_sql):
            df = fetch_stock_business_dataframe(fake_engine)

        self.assertEqual(list(df.columns), EXPORT_COLUMNS)
        self.assertEqual(
            df.to_dict("records"),
            [
                {
                    "股票代码": "000001.SZ",
                    "当前主要业务": "商业银行业务",
                    "当前产品及业务范围": "吸收公众存款；发放贷款。",
                },
                {
                    "股票代码": "600000.SH",
                    "当前主要业务": "",
                    "当前产品及业务范围": "",
                },
            ],
        )
        self.assertIs(calls[0][1], fake_engine)
        sql_text = calls[0][0]
        self.assertIn("vw_ts_stock_basic", sql_text)
        self.assertIn("vw_ts_stock_company", sql_text)
        self.assertIn("basic.list_status", sql_text)
        self.assertIn("company.main_business", sql_text)
        self.assertIn("company.business_scope", sql_text)

    def test_build_stock_business_sql_can_include_delisted_rows_when_requested(self):
        default_sql = build_stock_business_sql()
        all_sql = build_stock_business_sql(active_only=False)

        self.assertIn("basic.list_status", default_sql)
        self.assertNotIn("basic.list_status", all_sql)

    def test_export_stock_business_excel_writes_expected_workbook(self):
        df = pd.DataFrame(
            [
                {
                    "股票代码": "000001.SZ",
                    "当前主要业务": "商业银行业务",
                    "当前产品及业务范围": "吸收公众存款；发放贷款。",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = export_stock_business_excel(
                df,
                output_dir=Path(tmp_dir),
                run_date=date(2026, 6, 21),
            )
            exported = pd.read_excel(output_path)

        self.assertEqual(output_path.name, "个股业务范围_20260621.xlsx")
        self.assertEqual(list(exported.columns), EXPORT_COLUMNS)
        self.assertEqual(exported.iloc[0]["股票代码"], "000001.SZ")
        self.assertEqual(exported.iloc[0]["当前主要业务"], "商业银行业务")

    def test_script_parse_args_supports_output_dir_and_delisted_flag(self):
        args = parse_args(["--output-dir", "out/stock_business", "--include-delisted", "--json"])

        self.assertEqual(args.output_dir, "out/stock_business")
        self.assertTrue(args.include_delisted)
        self.assertTrue(args.json)

    def test_script_main_exports_database_rows(self):
        with patch("scripts.export_stock_business_to_excel.export_stock_business_from_database") as exporter:
            exporter.return_value = (Path("out/个股业务范围_20260621.xlsx"), 2)

            exit_code = main(["--output-dir", "out", "--json"])

        self.assertEqual(exit_code, 0)
        exporter.assert_called_once_with(output_dir="out", active_only=True)

    def test_vps_runner_script_calls_exporter_with_project_virtualenv(self):
        script = (PROJECT_ROOT / "systemd" / "etf-stock-business-daily.sh").read_text(encoding="utf-8")

        self.assertIn("export_stock_business_to_excel.py", script)
        self.assertIn("backup_stock_business_to_github.sh", script)
        self.assertIn("/opt/etf-app", script)
        self.assertIn(".venv/bin/python", script)
        self.assertIn("PYTHONPATH", script)
        self.assertIn("output", script)
        self.assertIn("stock_business", script)
        self.assertIn("logs", script)

    def test_vps_systemd_timer_runs_daily_at_21_shanghai_time(self):
        service = (PROJECT_ROOT / "systemd" / "etf-stock-business-daily.service").read_text(encoding="utf-8")
        timer = (PROJECT_ROOT / "systemd" / "etf-stock-business-daily.timer").read_text(encoding="utf-8")

        self.assertIn("ExecStart=/usr/local/bin/etf-stock-business-daily.sh", service)
        self.assertIn("WorkingDirectory=/opt/etf-app", service)
        self.assertIn("OnCalendar=*-*-* 13:00:00 UTC", timer)
        self.assertIn("Persistent=true", timer)
        self.assertIn("Unit=etf-stock-business-daily.service", timer)

    def test_github_backup_script_uses_separate_clone_and_env_config(self):
        script = (PROJECT_ROOT / "scripts" / "backup_stock_business_to_github.sh").read_text(encoding="utf-8")

        self.assertIn("STOCK_BUSINESS_BACKUP_GIT_URL", script)
        self.assertIn("STOCK_BUSINESS_BACKUP_DIR", script)
        self.assertIn("git clone", script)
        self.assertIn("git pull --ff-only", script)
        self.assertIn("git commit", script)
        self.assertIn("git push origin", script)
        self.assertIn("output/stock_business", script)
