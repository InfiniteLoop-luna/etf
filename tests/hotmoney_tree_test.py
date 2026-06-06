import unittest

import pandas as pd
from bs4 import BeautifulSoup

from src.hotmoney_tree import build_hotmoney_tree_model, render_hotmoney_tree_html


class HotmoneyTreeTests(unittest.TestCase):
    def test_build_hotmoney_tree_model_groups_daily_rows(self):
        detail_df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-06-05",
                    "hm_name": "量化打板",
                    "ts_name": "华林证券",
                    "ts_code": "002945.SZ",
                    "hm_orgs": "华鑫证券上海分公司",
                    "net_amount_yi": 0.2606,
                    "abs_net_amount_yi": 0.2606,
                },
                {
                    "trade_date": "2026-06-05",
                    "hm_name": "量化打板",
                    "ts_name": "华星科技",
                    "ts_code": "301000.SZ",
                    "hm_orgs": "华鑫证券上海分公司",
                    "net_amount_yi": -0.2503,
                    "abs_net_amount_yi": 0.2503,
                },
                {
                    "trade_date": "2026-06-05",
                    "hm_name": "广东帮",
                    "ts_name": "首创证券",
                    "ts_code": "601136.SH",
                    "hm_orgs": "财通证券温岭中华路",
                    "net_amount_yi": -0.7753,
                    "abs_net_amount_yi": 0.7753,
                },
            ]
        )

        model = build_hotmoney_tree_model(detail_df, max_hotmoney=2, max_stocks_per_hotmoney=2)

        self.assertEqual(model["trade_date_label"], "2026-06-05")
        self.assertEqual(model["total_records"], 3)
        self.assertEqual([group["hm_name"] for group in model["groups"]], ["广东帮", "量化打板"])
        self.assertEqual(model["groups"][0]["stocks"][0]["amount_label"], "-7753万")
        self.assertEqual(model["groups"][1]["stocks"][0]["orgs"][0]["label"], "华鑫证券上海分公司 +2606万")

    def test_render_hotmoney_tree_html_uses_dom_nodes_not_images(self):
        detail_df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-06-05",
                    "hm_name": "T王",
                    "ts_name": "国富科技",
                    "ts_code": "688123.SH",
                    "hm_orgs": "东方财富证券拉萨东环路二",
                    "net_amount_yi": 1.14,
                    "abs_net_amount_yi": 1.14,
                }
            ]
        )

        html = render_hotmoney_tree_html(detail_df, title="游资龙虎图谱", subtitle="最近5日 · Top20")
        soup = BeautifulSoup(html, "html.parser")

        self.assertIsNotNone(soup.select_one(".ws-hotmoney-tree"))
        self.assertIsNotNone(soup.select_one(".ws-hotmoney-tree-root"))
        self.assertEqual(len(soup.select(".ws-hotmoney-hm-node")), 1)
        self.assertEqual(len(soup.select(".ws-hotmoney-stock-node.is-positive")), 1)
        self.assertEqual(len(soup.select(".ws-hotmoney-org-node")), 1)
        self.assertIn("游资龙虎图谱", soup.get_text())
        self.assertIn("国富科技 +1.14亿", soup.get_text())
        self.assertIn("东方财富证券拉萨东环路二 +1.14亿", soup.get_text())
        self.assertEqual(soup.select("img"), [])
        self.assertNotIn("下载 PNG 图片", soup.get_text())


if __name__ == "__main__":
    unittest.main()
