import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.stock_research_llm_analysis import analyze_stock_research_payload, normalize_stock_research_llm_result


class StockResearchLLMAnalysisTests(unittest.TestCase):
    @patch("src.stock_research_llm_analysis.requests.post")
    def test_analyze_stock_research_payload_requests_json_object_response(self, mock_post):
        config = SimpleNamespace(
            configured=True,
            base_url="https://api.deepseek.com",
            api_key="sk-test",
            model="deepseek-v4-flash",
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
        self.assertEqual(request_payload["thinking"], {"type": "disabled"})
        self.assertIn("JSON 输出样例", request_payload["messages"][0]["content"])
        self.assertEqual(result["verdict"], "观察")
        self.assertEqual(result["model"], "deepseek-v4-flash")

    def test_normalize_stock_research_result_maps_v4_pro_aliases(self):
        normalized = normalize_stock_research_llm_result(
            {
                "verdict": "观望",
                "risk_level": "medium",
                "confidence": "中",
                "quality_score": {"score": "高", "grade": "A"},
                "step_analysis": {"step0": "观察"},
            }
        )

        self.assertEqual(normalized["verdict"], "观察")
        self.assertEqual(normalized["risk_level"], "中")
        self.assertEqual(normalized["confidence"], 50)
        self.assertEqual(normalized["quality_score"]["score"], 75)


if __name__ == "__main__":
    unittest.main()
