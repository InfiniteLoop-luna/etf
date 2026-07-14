import unittest

import pandas as pd
import plotly.graph_objects as go

from app import (
    apply_time_series_hover_affordance,
    create_metric_line_chart,
    create_volume_total_line,
)


class PlotlyTimeSeriesHoverAffordanceTests(unittest.TestCase):
    def test_helper_adds_right_edge_hover_affordance_to_daily_series(self):
        dates = pd.to_datetime(["2026-06-19", "2026-06-22"])
        fig = go.Figure(
            go.Scatter(
                x=dates,
                y=[10.0, 12.0],
                mode="lines",
                name="demo",
            )
        )
        fig.update_layout(hovermode="x unified", margin=dict(l=20, r=20, t=20, b=20))

        apply_time_series_hover_affordance(fig, dates, [10.0, 12.0])

        self.assertGreaterEqual(fig.layout.margin.r, 140)
        self.assertGreaterEqual(fig.layout.hoverdistance, 40)
        self.assertIsNotNone(fig.layout.xaxis.range)
        self.assertGreaterEqual(pd.Timestamp(fig.layout.xaxis.range[1]), dates.max() + pd.Timedelta(days=30))
        self.assertTrue(any(pd.Timestamp(shape.x0) == dates.max() for shape in fig.layout.shapes))
        self.assertTrue(any(dates.max().strftime("%Y-%m-%d") in annotation.text for annotation in fig.layout.annotations))
        hover_targets = [trace for trace in fig.data if getattr(trace, "name", None) == "latest-day-hover-target"]
        self.assertEqual(len(hover_targets), 1)
        self.assertGreaterEqual(hover_targets[0].line.width, 30)

    def test_helper_uses_minute_padding_for_intraday_series(self):
        times = pd.to_datetime(["2026-06-22 09:30:00", "2026-06-22 14:59:00"])
        fig = go.Figure(go.Scatter(x=times, y=[10.0, 11.0], mode="lines"))
        fig.update_layout(hovermode="x unified", margin=dict(l=20, r=20, t=20, b=20))

        apply_time_series_hover_affordance(
            fig,
            times,
            [10.0, 11.0],
            min_right_pad=pd.Timedelta(minutes=20),
        )

        self.assertGreaterEqual(pd.Timestamp(fig.layout.xaxis.range[1]), times.max() + pd.Timedelta(minutes=20))
        self.assertLess(pd.Timestamp(fig.layout.xaxis.range[1]), times.max() + pd.Timedelta(days=1))

    def test_volume_total_line_gets_latest_day_affordance(self):
        df = pd.DataFrame(
            [
                {"trade_date": pd.Timestamp("2026-06-19"), "amount": 1000.0, "vol": 10.0},
                {"trade_date": pd.Timestamp("2026-06-22"), "amount": 1200.0, "vol": 12.0},
            ]
        )

        fig = create_volume_total_line(df)

        self.assertGreaterEqual(fig.layout.hoverdistance, 40)
        self.assertIsNotNone(fig.layout.xaxis.range)
        self.assertGreaterEqual(pd.Timestamp(fig.layout.xaxis.range[1]), df["trade_date"].max() + pd.Timedelta(days=30))
        self.assertTrue(any(df["trade_date"].max().strftime("%Y-%m-%d") in annotation.text for annotation in fig.layout.annotations))

    def test_metric_line_chart_gets_latest_day_affordance(self):
        df = pd.DataFrame(
            [
                {"trade_date": pd.Timestamp("2026-06-19"), "value": 10.0},
                {"trade_date": pd.Timestamp("2026-06-22"), "value": 12.0},
            ]
        )

        fig = create_metric_line_chart(df, "trade_date", "value", "demo", "value")

        self.assertGreaterEqual(fig.layout.hoverdistance, 40)
        self.assertIsNotNone(fig.layout.xaxis.range)
        self.assertGreaterEqual(pd.Timestamp(fig.layout.xaxis.range[1]), df["trade_date"].max() + pd.Timedelta(days=30))


if __name__ == "__main__":
    unittest.main()
