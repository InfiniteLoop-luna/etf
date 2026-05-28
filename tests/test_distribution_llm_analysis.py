import json
import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from src.distribution_llm_analysis import (
    LLM_SECTION_MARKER,
    LLM_SCHEMA_VERSION,
    analyze_distribution_payload,
    make_json_safe,
    load_distribution_llm_config,
    normalize_distribution_llm_result,
    parse_llm_json_object,
    render_distribution_llm_markdown,
)


class DistributionLLMAnalysisTests(unittest.TestCase):
    def test_make_json_safe_converts_nested_non_json_values(self):
        payload = {
            "trade_date": date(2026, 5, 27),
            "generated_at": datetime(2026, 5, 27, 17, 30, 0),
            "nan_value": float("nan"),
            "inf_value": float("inf"),
            "nested": [
                pd.Timestamp("2026-05-27 09:30:00"),
                {"neg_inf": float("-inf")},
            ],
        }

        safe_payload = make_json_safe(payload)

        self.assertEqual(safe_payload["trade_date"], "2026-05-27")
        self.assertEqual(safe_payload["generated_at"], "2026-05-27T17:30:00")
        self.assertIsNone(safe_payload["nan_value"])
        self.assertIsNone(safe_payload["inf_value"])
        self.assertEqual(safe_payload["nested"][0], "2026-05-27T09:30:00")
        self.assertIsNone(safe_payload["nested"][1]["neg_inf"])

        json.dumps(safe_payload, ensure_ascii=False)

    def test_load_distribution_llm_config_prefers_env_file_key_over_secrets_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / '.env').write_text(
                'DISTRIBUTION_LLM_ENABLED=true\n'
                'DISTRIBUTION_LLM_API_KEY=sk-real-ascii-key\n',
                encoding='utf-8',
            )
            (root / '.streamlit').mkdir(parents=True, exist_ok=True)
            (root / '.streamlit' / 'secrets.toml').write_text(
                'DISTRIBUTION_LLM_ENABLED = true\n'
                'DISTRIBUTION_LLM_API_KEY = "sk-15b…271a"\n',
                encoding='utf-8',
            )
            with patch('src.distribution_llm_analysis.ENV_PATH', root / '.env'), \
                 patch('src.distribution_llm_analysis.SECRETS_PATH', root / '.streamlit' / 'secrets.toml'), \
                 patch.dict(os.environ, {}, clear=True):
                from src import distribution_llm_analysis as module
                module._load_env_file.cache_clear()
                cfg = load_distribution_llm_config()

        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.base_url, 'https://api.deepseek.com')
        self.assertEqual(cfg.model, 'deepseek-v4-pro')
        self.assertEqual(cfg.api_key, 'sk-real-ascii-key')

    def test_load_distribution_llm_config_skips_bad_primary_key_and_falls_back_to_alt_env_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / '.env').write_text(
                'DISTRIBUTION_LLM_ENABLED=true\n'
                'DISTRIBUTION_LLM_API_KEY=sk-bad…key\n'
                'DEEPSEEK_API_KEY=sk-good-ascii-key\n',
                encoding='utf-8',
            )
            with patch('src.distribution_llm_analysis.ENV_PATH', root / '.env'), \
                 patch('src.distribution_llm_analysis.SECRETS_PATH', root / '.streamlit' / 'secrets.toml'), \
                 patch.dict(os.environ, {}, clear=True):
                from src import distribution_llm_analysis as module
                module._load_env_file.cache_clear()
                cfg = load_distribution_llm_config()

        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.api_key, 'sk-good-ascii-key')

    def test_load_distribution_llm_config_rejects_non_ascii_secret_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / '.streamlit').mkdir(parents=True, exist_ok=True)
            (root / '.streamlit' / 'secrets.toml').write_text(
                'DISTRIBUTION_LLM_ENABLED = true\n'
                'DISTRIBUTION_LLM_API_KEY = "sk-15b…271a"\n',
                encoding='utf-8',
            )
            with patch('src.distribution_llm_analysis.ENV_PATH', root / '.env'), \
                 patch('src.distribution_llm_analysis.SECRETS_PATH', root / '.streamlit' / 'secrets.toml'), \
                 patch.dict(os.environ, {}, clear=True):
                from src import distribution_llm_analysis as module
                module._load_env_file.cache_clear()
                cfg = load_distribution_llm_config()

        self.assertEqual(cfg.api_key, '')
        self.assertFalse(cfg.configured)

    def test_should_require_llm_refresh_requires_current_professional_schema_marker(self):
        from src.distribution_llm_analysis import should_require_llm_refresh

        self.assertTrue(should_require_llm_refresh('# cached report'))
        self.assertTrue(should_require_llm_refresh('# cached report\n\n' + LLM_SECTION_MARKER))
        self.assertFalse(should_require_llm_refresh('# cached report\n\n' + LLM_SECTION_MARKER + '\n' + LLM_SCHEMA_VERSION))

    def test_parse_llm_json_object_accepts_fenced_json(self):
        content = '```json\n{"verdict":"疑似出货","confidence":75}\n```'
        parsed = parse_llm_json_object(content)
        self.assertEqual(parsed['verdict'], '疑似出货')
        self.assertEqual(parsed['confidence'], 75)

    def test_parse_llm_json_object_extracts_first_balanced_object_from_noisy_text(self):
        content = '先看结论如下：\n{"verdict":"中性","confidence":55,"summary":"震荡"}\n补充说明忽略'
        parsed = parse_llm_json_object(content)
        self.assertEqual(parsed['verdict'], '中性')
        self.assertEqual(parsed['confidence'], 55)

    def test_normalize_distribution_llm_result_clamps_and_defaults_schema(self):
        normalized = normalize_distribution_llm_result(
            {
                "verdict": "超级确定",
                "risk_level": "极高",
                "confidence": 180,
                "summary": "偏离允许枚举时应兜底",
                "evidence_for": ["A", "B", "C", "D", "E"],
                "scenario_analysis": ["情景1", "情景2", "情景3", "情景4"],
            }
        )

        self.assertEqual(normalized["verdict"], "中性")
        self.assertEqual(normalized["risk_level"], "低")
        self.assertEqual(normalized["confidence"], 100)
        self.assertEqual(normalized["evidence_for"], ["A", "B", "C", "D"])
        self.assertEqual(normalized["scenario_analysis"], ["情景1", "情景2", "情景3"])

    def test_render_distribution_llm_markdown_outputs_professional_sections(self):
        lines = render_distribution_llm_markdown(
            {
                "verdict": "疑似出货",
                "risk_level": "中",
                "confidence": 72,
                "summary": "高位信号增多，但反证仍在。",
                "professional_view": "短线需要观察放量下跌后的承接力度。",
                "evidence_for": ["放量下跌后未能快速修复"],
                "evidence_against": ["分笔数据缺失降低确认度"],
                "key_levels": ["若跌破 MA20 后不能收回，风险升高"],
                "scenario_analysis": ["放量跌破支撑则偏强出货"],
                "watch_items": ["后续 1-3 日成交量是否继续放大"],
                "action_suggestion": ["控制单票暴露，等待确认信号"],
                "data_quality_note": "tick/minute 覆盖不足。",
            }
        )
        markdown = "\n".join(lines)

        self.assertIn(LLM_SECTION_MARKER, markdown)
        self.assertIn("风险等级", markdown)
        self.assertIn(LLM_SCHEMA_VERSION, markdown)
        self.assertIn("专业解读", markdown)
        self.assertIn("关键价量观察位", markdown)
        self.assertIn("风控与操作提示", markdown)
        self.assertIn("不构成确定性交易指令", markdown)

    @patch('src.distribution_llm_analysis.requests.post')
    def test_analyze_distribution_payload_retries_once_on_non_json_then_succeeds(self, mock_post):
        config = SimpleNamespace(
            configured=True,
            base_url='https://api.deepseek.com',
            api_key='sk-test',
            model='deepseek-v4-flash',
            timeout_seconds=30,
            temperature=0.2,
            max_tokens=1200,
        )

        bad = Mock()
        bad.raise_for_status.return_value = None
        bad.json.return_value = {
            'choices': [{'message': {'content': '结论如下：不是纯json'}}]
        }
        good = Mock()
        good.raise_for_status.return_value = None
        good.json.return_value = {
            'choices': [{'message': {'content': '{"verdict":"疑似出货","confidence":71}'}}]
        }
        mock_post.side_effect = [bad, good]

        result = analyze_distribution_payload({'ts_code': '000733.SZ'}, config=config)

        self.assertEqual(result['verdict'], '疑似出货')
        self.assertEqual(result['confidence'], 71)
        self.assertEqual(mock_post.call_count, 2)

    @patch('src.distribution_llm_analysis.requests.post')
    def test_analyze_distribution_payload_returns_none_when_both_attempts_invalid(self, mock_post):
        config = SimpleNamespace(
            configured=True,
            base_url='https://api.deepseek.com',
            api_key='sk-test',
            model='deepseek-v4-flash',
            timeout_seconds=30,
            temperature=0.2,
            max_tokens=1200,
        )

        empty = Mock()
        empty.raise_for_status.return_value = None
        empty.json.return_value = {'choices': [{'message': {'content': ''}}]}
        noisy = Mock()
        noisy.raise_for_status.return_value = None
        noisy.json.return_value = {'choices': [{'message': {'content': '不是json'}}]}
        mock_post.side_effect = [empty, noisy]

        result = analyze_distribution_payload({'ts_code': '000733.SZ'}, config=config)

        self.assertIsNone(result)
        self.assertEqual(mock_post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
