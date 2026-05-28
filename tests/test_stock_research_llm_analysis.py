import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.stock_research_llm_analysis import analyze_stock_research_payload


class StockResearchLLMAnalysisTests(unittest.TestCase):
    @patch("src.stock_research_llm_analysis.requests.post")
    def test_analyze_stock_research_payload_requests_json_object_response(self, mock_post):
        config = SimpleNamespace(
            configured=True,
            base_url="https://api.deepseek.com",
            api_key="sk-test",
            model="deepseek-v4-pro",
            timeout_seconds=30,
            temperature=0.2,
            max_tokens=1200,
        )
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": (
                            '{"verdict":"观察","risk_level":"中","confidence":60,'
                            '"quality_score":{"score":55,"grade":"C"},'
                            '"step_analysis":{"step0":"锁定自选股"}}'
                        )
                    }
                }
            ]
        }
        mock_post.return_value = response

        result = analyze_stock_research_payload({"ts_code": "000733.SZ"}, config=config)

        request_payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(request_payload["response_format"], {"type": "json_object"})
        self.assertEqual(result["verdict"], "观察")
        self.assertEqual(result["model"], "deepseek-v4-pro")


if __name__ == "__main__":
    unittest.main()
