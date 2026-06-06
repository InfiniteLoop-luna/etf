import io
import unittest

import pandas as pd
from PIL import Image

from src.hotmoney_image import build_hotmoney_image_model, render_hotmoney_daily_image


class HotmoneyImageTests(unittest.TestCase):
    def test_build_hotmoney_image_model_groups_hotmoney_stocks_and_orgs(self):
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

        model = build_hotmoney_image_model(detail_df, max_hotmoney=2, max_stocks_per_hotmoney=2)

        self.assertEqual(model["total_records"], 3)
        self.assertEqual(model["trade_date_label"], "2026-06-05")
        self.assertEqual([group["hm_name"] for group in model["groups"]], ["广东帮", "量化打板"])
        first_stock = model["groups"][0]["stocks"][0]
        self.assertEqual(first_stock["stock_name"], "首创证券")
        self.assertEqual(first_stock["amount_label"], "-7753万")
        self.assertEqual(first_stock["orgs"][0]["label"], "财通证券温岭中华路 -7753万")

    def test_render_hotmoney_daily_image_returns_readable_png(self):
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

        image_bytes = render_hotmoney_daily_image(detail_df, title="游资龙虎榜")

        self.assertTrue(image_bytes.startswith(b"\x89PNG\r\n\x1a\n"))
        with Image.open(io.BytesIO(image_bytes)) as image:
            self.assertEqual(image.format, "PNG")
            self.assertGreaterEqual(image.width, 900)
            self.assertGreaterEqual(image.height, 260)


if __name__ == "__main__":
    unittest.main()
