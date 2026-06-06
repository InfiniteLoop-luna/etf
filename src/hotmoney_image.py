# -*- coding: utf-8 -*-
"""Render hot-money daily detail rows as a shareable PNG mind map."""

from __future__ import annotations

import io
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd
from PIL import Image, ImageDraw, ImageFont


_BG = "#FFFFFF"
_ROOT_FILL = "#2EBBD2"
_ROOT_TEXT = "#FFFFFF"
_CONNECTOR = "#28D9C5"
_CONNECTOR_DARK = "#40524E"
_TEXT = "#0E4A4C"
_TEXT_DARK = "#263A38"
_POSITIVE = "#F20F0F"
_NEGATIVE = "#11822B"
_NEUTRAL = "#65767A"
_ORG_FILL = "#D9FAF5"
_ORG_FILL_ALT = "#DDEFE0"
_GROUP_FILLS = ["#09AEA8", "#0F9EA0", "#3E534E", "#2EDFD1"]


def _clean_text(value: Any, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _to_numeric(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(num) else num


def _trim_number(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_amount_label(amount_yi: float) -> str:
    amount_yi = _to_numeric(amount_yi)
    sign = "+" if amount_yi > 0 else "-" if amount_yi < 0 else ""
    abs_yi = abs(amount_yi)
    if abs_yi >= 1:
        return f"{sign}{_trim_number(abs_yi)}亿"
    return f"{sign}{int(round(abs_yi * 10000))}万"


def _split_orgs(value: Any, max_items: int) -> list[str]:
    text = _clean_text(value, "")
    if not text:
        return ["关联席位未披露"]
    parts = [part.strip() for part in re.split(r"[、,，;；|/]+", text) if part.strip()]
    deduped: list[str] = []
    for part in parts:
        if part not in deduped:
            deduped.append(part)
        if len(deduped) >= max_items:
            break
    return deduped or ["关联席位未披露"]


def _prepare_image_detail_frame(detail_df: pd.DataFrame | None) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "hm_name",
                "ts_name",
                "ts_code",
                "hm_orgs",
                "net_amount_yi",
                "abs_net_amount_yi",
            ]
        )

    work = detail_df.copy()
    work["trade_date"] = pd.to_datetime(work.get("trade_date"), errors="coerce")
    work["hm_name"] = work.get("hm_name", "").map(lambda value: _clean_text(value, "未知游资"))
    work["ts_code"] = work.get("ts_code", "").map(lambda value: _clean_text(value, "-"))
    work["ts_name"] = work.get("ts_name", "").map(lambda value: _clean_text(value, ""))
    work["ts_name"] = work.apply(lambda row: row["ts_name"] or row["ts_code"], axis=1)
    work["hm_orgs"] = work.get("hm_orgs", "").map(lambda value: _clean_text(value, ""))

    if "net_amount_yi" in work.columns:
        work["net_amount_yi"] = pd.to_numeric(work["net_amount_yi"], errors="coerce").fillna(0.0)
    elif "net_amount" in work.columns:
        work["net_amount_yi"] = pd.to_numeric(work["net_amount"], errors="coerce").fillna(0.0) / 1e8
    else:
        work["net_amount_yi"] = 0.0
    work["abs_net_amount_yi"] = work["net_amount_yi"].abs()
    return work


def _trade_date_label(work: pd.DataFrame) -> str:
    dates = work["trade_date"].dropna()
    if dates.empty:
        return "-"
    start = dates.min().strftime("%Y-%m-%d")
    end = dates.max().strftime("%Y-%m-%d")
    return start if start == end else f"{start}~{end}"


def build_hotmoney_image_model(
    detail_df: pd.DataFrame | None,
    *,
    max_hotmoney: int = 8,
    max_stocks_per_hotmoney: int = 6,
    max_orgs_per_stock: int = 5,
) -> dict[str, Any]:
    """Build a deterministic tree model for the hot-money PNG renderer."""
    work = _prepare_image_detail_frame(detail_df)
    if work.empty:
        return {"trade_date_label": "-", "total_records": 0, "groups": []}

    row_summary = (
        work.groupby(["hm_name", "ts_code", "ts_name", "hm_orgs"], dropna=False)
        .agg(
            net_amount_yi=("net_amount_yi", "sum"),
            abs_net_amount_yi=("abs_net_amount_yi", "sum"),
            hit_count=("hm_name", "size"),
        )
        .reset_index()
    )
    hm_rank = (
        row_summary.groupby("hm_name", dropna=False)
        .agg(
            total_abs_yi=("abs_net_amount_yi", "sum"),
            total_net_yi=("net_amount_yi", "sum"),
            hit_count=("hit_count", "sum"),
            stock_count=("ts_code", "nunique"),
        )
        .sort_values(["total_abs_yi", "hit_count"], ascending=[False, False])
        .head(max(1, int(max_hotmoney)))
        .reset_index()
    )

    groups: list[dict[str, Any]] = []
    for _, hm_row in hm_rank.iterrows():
        hm_name = _clean_text(hm_row["hm_name"], "未知游资")
        hm_rows = row_summary[row_summary["hm_name"] == hm_name].copy()
        stock_rank = (
            hm_rows.groupby(["ts_code", "ts_name"], dropna=False)
            .agg(
                net_amount_yi=("net_amount_yi", "sum"),
                abs_net_amount_yi=("abs_net_amount_yi", "sum"),
                hit_count=("hit_count", "sum"),
            )
            .sort_values(["abs_net_amount_yi", "hit_count"], ascending=[False, False])
            .head(max(1, int(max_stocks_per_hotmoney)))
            .reset_index()
        )

        stocks: list[dict[str, Any]] = []
        for _, stock_row in stock_rank.iterrows():
            ts_code = _clean_text(stock_row["ts_code"], "-")
            stock_name = _clean_text(stock_row["ts_name"], ts_code)
            stock_rows = hm_rows[(hm_rows["ts_code"] == ts_code) & (hm_rows["ts_name"] == stock_name)]
            stock_rows = stock_rows.sort_values(["abs_net_amount_yi", "hit_count"], ascending=[False, False])
            orgs: list[dict[str, str]] = []
            for _, org_row in stock_rows.iterrows():
                amount_label = _format_amount_label(org_row["net_amount_yi"])
                for org in _split_orgs(org_row["hm_orgs"], max_items=max_orgs_per_stock):
                    orgs.append({"name": org, "label": f"{org} {amount_label}"})
                    if len(orgs) >= max_orgs_per_stock:
                        break
                if len(orgs) >= max_orgs_per_stock:
                    break
            if not orgs:
                orgs.append({"name": "关联席位未披露", "label": f"关联席位未披露 {_format_amount_label(stock_row['net_amount_yi'])}"})

            amount_yi = _to_numeric(stock_row["net_amount_yi"])
            stocks.append(
                {
                    "stock_name": stock_name,
                    "ts_code": ts_code,
                    "amount_yi": amount_yi,
                    "amount_label": _format_amount_label(amount_yi),
                    "hit_count": int(stock_row["hit_count"]),
                    "orgs": orgs,
                }
            )

        groups.append(
            {
                "hm_name": hm_name,
                "total_abs_yi": _to_numeric(hm_row["total_abs_yi"]),
                "total_net_yi": _to_numeric(hm_row["total_net_yi"]),
                "hit_count": int(hm_row["hit_count"]),
                "stock_count": int(hm_row["stock_count"]),
                "stocks": stocks,
            }
        )

    return {
        "trade_date_label": _trade_date_label(work),
        "total_records": int(len(work)),
        "groups": groups,
    }


def _font_candidates(bold: bool) -> list[str]:
    windows = [
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\Dengb.ttf" if bold else r"C:\Windows\Fonts\Deng.ttf",
    ]
    mac = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
    ]
    linux = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    return windows + mac + linux


def _load_font(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    for candidate in _font_candidates(bold):
        if Path(candidate).exists():
            try:
                return ImageFont.truetype(candidate, size=size)
            except OSError:
                continue
    try:
        return ImageFont.truetype("DejaVuSans.ttf", size=size)
    except OSError:
        return ImageFont.load_default()


def _text_size(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _fit_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    text = str(text)
    if _text_size(draw, text, font)[0] <= max_width:
        return text
    suffix = "..."
    trimmed = text
    while trimmed and _text_size(draw, f"{trimmed}{suffix}", font)[0] > max_width:
        trimmed = trimmed[:-1]
    return f"{trimmed}{suffix}" if trimmed else suffix


def _draw_label(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    text: str,
    *,
    fill: str,
    font: ImageFont.ImageFont,
    text_fill: str,
    radius: int = 10,
    pad_x: int = 12,
) -> None:
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle(xy, radius=radius, fill=fill)
    fitted = _fit_text(draw, text, font, max(10, x2 - x1 - pad_x * 2))
    text_w, text_h = _text_size(draw, fitted, font)
    draw.text((x1 + pad_x, y1 + (y2 - y1 - text_h) / 2 - 1), fitted, font=font, fill=text_fill)


def _draw_root(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    title: str,
    date_label: str,
    *,
    title_font: ImageFont.ImageFont,
    date_font: ImageFont.ImageFont,
) -> None:
    draw.rounded_rectangle(xy, radius=8, fill=_ROOT_FILL)
    x1, y1, x2, y2 = xy
    lines = [title, f"({date_label})" if date_label and date_label != "-" else ""]
    heights = [_text_size(draw, line, title_font if idx == 0 else date_font)[1] for idx, line in enumerate(lines) if line]
    total_h = sum(heights) + (8 if len(heights) > 1 else 0)
    y = y1 + (y2 - y1 - total_h) / 2
    for idx, line in enumerate(lines):
        if not line:
            continue
        font = title_font if idx == 0 else date_font
        fitted = _fit_text(draw, line, font, x2 - x1 - 32)
        text_w, text_h = _text_size(draw, fitted, font)
        draw.text((x1 + (x2 - x1 - text_w) / 2, y), fitted, font=font, fill=_ROOT_TEXT)
        y += text_h + 8


def _build_leaf_layout(model: dict[str, Any], row_gap: int, group_gap: int, top: int) -> tuple[list[dict[str, Any]], int]:
    layout: list[dict[str, Any]] = []
    y = top
    for group_index, group in enumerate(model["groups"]):
        group_leaf_ys: list[int] = []
        stock_layouts: list[dict[str, Any]] = []
        for stock in group["stocks"]:
            org_ys: list[int] = []
            for org in stock["orgs"]:
                org_ys.append(y)
                group_leaf_ys.append(y)
                y += row_gap
            stock_layouts.append({"stock": stock, "org_ys": org_ys})
        if group_leaf_ys:
            group_center = int((min(group_leaf_ys) + max(group_leaf_ys)) / 2)
            layout.append(
                {
                    "group": group,
                    "group_index": group_index,
                    "center_y": group_center,
                    "min_y": min(group_leaf_ys),
                    "max_y": max(group_leaf_ys),
                    "stocks": stock_layouts,
                }
            )
            y += group_gap
    return layout, y


def render_hotmoney_daily_image(
    detail_df: pd.DataFrame | None,
    *,
    title: str = "游资龙虎图谱",
    subtitle: str | None = None,
    max_hotmoney: int = 8,
    max_stocks_per_hotmoney: int = 6,
    max_orgs_per_stock: int = 5,
) -> bytes:
    """Render hot-money detail rows to PNG bytes."""
    model = build_hotmoney_image_model(
        detail_df,
        max_hotmoney=max_hotmoney,
        max_stocks_per_hotmoney=max_stocks_per_hotmoney,
        max_orgs_per_stock=max_orgs_per_stock,
    )
    row_gap = 52
    group_gap = 44
    top = 86 if subtitle else 66
    layout, bottom_y = _build_leaf_layout(model, row_gap=row_gap, group_gap=group_gap, top=top)
    width = 1220
    height = max(260, bottom_y + 40)

    image = Image.new("RGB", (width, height), _BG)
    draw = ImageDraw.Draw(image)

    title_font = _load_font(26, bold=True)
    root_date_font = _load_font(21, bold=True)
    group_font = _load_font(25, bold=True)
    stock_font = _load_font(21, bold=True)
    org_font = _load_font(21)
    subtitle_font = _load_font(17)
    empty_font = _load_font(22)

    if subtitle:
        draw.text((50, 26), _fit_text(draw, subtitle, subtitle_font, width - 100), font=subtitle_font, fill="#5B6D70")

    if not layout:
        _draw_root(draw, (48, 72, 314, 186), title, "-", title_font=title_font, date_font=root_date_font)
        draw.text((390, 116), "当前筛选条件下暂无游资每日明细", font=empty_font, fill=_TEXT_DARK)
    else:
        all_leaf_ys = [leaf_y for group in layout for stock in group["stocks"] for leaf_y in stock["org_ys"]]
        root_center_y = int((min(all_leaf_ys) + max(all_leaf_ys)) / 2)
        root_box = (48, root_center_y - 58, 314, root_center_y + 58)
        _draw_root(
            draw,
            root_box,
            title,
            model.get("trade_date_label") or "-",
            title_font=title_font,
            date_font=root_date_font,
        )

        root_right = root_box[2]
        trunk_x = 352
        draw.line((root_right, root_center_y, trunk_x, root_center_y), fill=_CONNECTOR, width=4)
        draw.line((trunk_x, min(all_leaf_ys), trunk_x, max(all_leaf_ys)), fill=_CONNECTOR, width=4)

        group_x1, group_w, group_h = 390, 160, 64
        stock_x1, stock_w, stock_h = 590, 220, 42
        org_x1, org_w, org_h = 846, 330, 42

        for group_item in layout:
            group = group_item["group"]
            group_color = _GROUP_FILLS[group_item["group_index"] % len(_GROUP_FILLS)]
            center_y = group_item["center_y"]
            group_box = (group_x1, center_y - group_h // 2, group_x1 + group_w, center_y + group_h // 2)
            draw.line((trunk_x, center_y, group_box[0], center_y), fill=_CONNECTOR, width=4)
            _draw_label(
                draw,
                group_box,
                group["hm_name"],
                fill=group_color,
                font=group_font,
                text_fill="#FFFFFF",
                radius=9,
                pad_x=18,
            )

            stock_centers = [
                int((min(stock_item["org_ys"]) + max(stock_item["org_ys"])) / 2)
                for stock_item in group_item["stocks"]
                if stock_item["org_ys"]
            ]
            branch_x = group_box[2] + 22
            if stock_centers:
                draw.line((group_box[2], center_y, branch_x, center_y), fill=group_color, width=3)
                draw.line((branch_x, min(stock_centers), branch_x, max(stock_centers)), fill=group_color, width=3)

            for stock_item in group_item["stocks"]:
                stock = stock_item["stock"]
                org_ys = stock_item["org_ys"]
                stock_center = int((min(org_ys) + max(org_ys)) / 2)
                amount_yi = _to_numeric(stock["amount_yi"])
                stock_fill = _POSITIVE if amount_yi > 0 else _NEGATIVE if amount_yi < 0 else _NEUTRAL
                stock_label = f"{stock['stock_name']} {stock['amount_label']}"
                stock_box = (stock_x1, stock_center - stock_h // 2, stock_x1 + stock_w, stock_center + stock_h // 2)
                draw.line((branch_x, stock_center, stock_box[0], stock_center), fill=group_color, width=3)
                _draw_label(
                    draw,
                    stock_box,
                    stock_label,
                    fill=stock_fill,
                    font=stock_font,
                    text_fill="#FFFFFF",
                    radius=9,
                    pad_x=12,
                )

                org_branch_x = stock_box[2] + 24
                if len(org_ys) > 1:
                    draw.line((stock_box[2], stock_center, org_branch_x, stock_center), fill=_CONNECTOR_DARK, width=2)
                    draw.line((org_branch_x, min(org_ys), org_branch_x, max(org_ys)), fill=_CONNECTOR_DARK, width=2)

                for org_index, (org, org_y) in enumerate(zip(stock["orgs"], org_ys)):
                    org_box = (org_x1, org_y - org_h // 2, org_x1 + org_w, org_y + org_h // 2)
                    line_start_x = org_branch_x if len(org_ys) > 1 else stock_box[2]
                    draw.line((line_start_x, org_y, org_box[0], org_y), fill=_CONNECTOR_DARK, width=2)
                    fill = _ORG_FILL if amount_yi >= 0 else _ORG_FILL_ALT
                    _draw_label(
                        draw,
                        org_box,
                        org["label"],
                        fill=fill,
                        font=org_font,
                        text_fill=_TEXT,
                        radius=8,
                        pad_x=12,
                    )

    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    return output.getvalue()
