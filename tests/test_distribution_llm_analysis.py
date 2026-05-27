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
    analyze_distribution_payload,
    make_json_safe,
    load_distribution_llm_config,
    parse_llm_json_object,
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
        self.assertEqual(cfg.model, 'deepseek-v4-flash')
        self.assertEqual(cfg.api_key, 'sk-real-ascii-key')

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

    def test_should_require_llm_refresh_only_checks_marker_presence(self):
        from src.distribution_llm_analysis import should_require_llm_refresh

        self.assertTrue(should_require_llm_refresh('# cached report'))
        self.assertFalse(should_require_llm_refresh('# cached report\n\n' + LLM_SECTION_MARKER))

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
