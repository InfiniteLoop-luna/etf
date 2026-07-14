import unittest

import pandas as pd

from app import create_volume_stacked_bar


def sample_volume_frame():
    sectors = [
        "\u6caa\u5e02\u4e3b\u677f",
        "\u6df1\u5e02\u4e3b\u677f",
        "\u521b\u4e1a\u677f",
        "\u79d1\u521b\u677f",
    ]
    rows = []
    for day, base_amount in [
        ("2026-06-19", 1000.0),
        ("2026-06-22", 1100.0),
    ]:
        for offset, sector in enumerate(sectors):
            rows.append(
                {
                    "trade_date": pd.Timestamp(day),
                    "ts_name": sector,
                    "amount": base_amount + offset * 100.0,
                }
            )
    return pd.DataFrame(rows)


class VolumeChartLayoutTests(unittest.TestCase):
    def test_stacked_bar_reserves_right_margin_for_unified_hover(self):
        fig = create_volume_stacked_bar(sample_volume_frame())

        self.assertEqual(fig.layout.hovermode, "x unified")
        self.assertGreaterEqual(fig.layout.hoverdistance, 40)
        self.assertGreaterEqual(fig.layout.margin.r, 140)

    def test_stacked_bar_adds_hoverable_space_after_latest_day(self):
        df = sample_volume_frame()

        fig = create_volume_stacked_bar(df)

        self.assertIsNotNone(fig.layout.xaxis.range)
        axis_end = pd.Timestamp(fig.layout.xaxis.range[1])
        latest_date = df["trade_date"].max()
        self.assertGreaterEqual(axis_end, latest_date + pd.Timedelta(days=30))

    def test_stacked_bar_marks_latest_day(self):
        df = sample_volume_frame()

        fig = create_volume_stacked_bar(df)

        latest_date = df["trade_date"].max()
        latest_lines = [
            shape
            for shape in fig.layout.shapes
            if pd.Timestamp(shape.x0) == latest_date and pd.Timestamp(shape.x1) == latest_date
        ]
        self.assertTrue(latest_lines)
        latest_annotations = [
            annotation
            for annotation in fig.layout.annotations
            if latest_date.strftime("%Y-%m-%d") in annotation.text
        ]
        self.assertTrue(latest_annotations)

    def test_stacked_bar_adds_latest_day_hover_target(self):
        df = sample_volume_frame()

        fig = create_volume_stacked_bar(df)

        latest_date = df["trade_date"].max()
        hover_targets = [
            trace
            for trace in fig.data
            if getattr(trace, "name", None) == "latest-day-hover-target"
        ]
        self.assertEqual(len(hover_targets), 1)
        target = hover_targets[0]
        self.assertEqual(target.mode, "lines")
        self.assertFalse(target.showlegend)
        self.assertGreaterEqual(target.line.width, 30)
        self.assertTrue(all(pd.Timestamp(x) == latest_date for x in target.x))


if __name__ == "__main__":
    unittest.main()
