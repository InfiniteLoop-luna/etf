from __future__ import annotations

import math
from html import escape
from typing import Any, Iterable

from src.etf_morning_report import build_report_digest


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _fmt(value: Any, digits: int = 2, suffix: str = "", *, signed: bool = False) -> str:
    number = _number(value)
    if number is None:
        return "--"
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:,.{digits}f}{suffix}"


def _text(value: Any, fallback: str = "--") -> str:
    cleaned = "" if value is None else str(value).strip()
    return escape(cleaned or fallback)


def _tone_class(value: Any) -> str:
    number = _number(value)
    if number is None or number == 0:
        return "is-neutral"
    return "is-positive" if number > 0 else "is-negative"


def _table_html(
    rows: Iterable[dict[str, Any]],
    columns: list[tuple[str, str]],
    *,
    empty_text: str = "暂无数据",
) -> str:
    rows = list(rows)
    if not rows:
        return f'<div class="mr-empty">{escape(empty_text)}</div>'
    head = "".join(f"<th>{escape(label)}</th>" for _, label in columns)
    body: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{_text(row.get(key))}</td>" for key, _ in columns)
        body.append(f"<tr>{cells}</tr>")
    return (
        '<div class="mr-table-wrap"><table><thead><tr>'
        f"{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"
    )


def _source_table_html(sources: list[dict[str, Any]]) -> str:
    status_labels = {"fresh": "已就绪", "stale": "滞后", "missing": "缺失"}
    rows = [
        {
            "source": source.get("label") or source.get("key") or "--",
            "role": "关键" if source.get("required") else "辅助",
            "status": status_labels.get(str(source.get("status") or "missing"), source.get("status") or "缺失"),
            "latest": source.get("latest_date") or "--",
            "count": source.get("row_count") or 0,
        }
        for source in sources
    ]
    return _table_html(
        rows,
        [("source", "数据源"), ("role", "角色"), ("status", "状态"), ("latest", "最近日期"), ("count", "当日记录")],
        empty_text="旧版报告没有数据源就绪记录",
    )


def _industry_groups_html(groups: list[dict[str, Any]]) -> str:
    if not groups:
        return '<div class="mr-empty">暂无行业 ETF 份额数据</div>'
    blocks: list[str] = []
    for group in groups:
        growth = _number(group.get("share_growth_pct"))
        growth_text = _fmt(growth, 2, "%", signed=True)
        etfs = [
            {
                "etf": f"{row.get('etf_name') or '--'}（{row.get('ts_code') or '--'}）",
                "growth": _fmt(row.get("share_growth_pct"), 2, "%", signed=True),
                "change": _fmt(row.get("share_change_yi"), 2, " 亿份", signed=True),
                "current": _fmt(row.get("current_share_yi"), 2, " 亿份"),
            }
            for row in (group.get("etfs") or [])
        ]
        summary = (
            f'<span class="mr-industry-name">{_text(group.get("industry"), "未识别行业")}</span>'
            f'<span class="mr-industry-count">{int(_number(group.get("etf_count")) or 0)} 只 ETF</span>'
            f'<span class="mr-value {_tone_class(growth)}">较前一日 {_text(growth_text)}</span>'
            f'<span class="mr-industry-change">份额增减 {_text(_fmt(group.get("share_change_yi"), 2, " 亿份", signed=True))}'
            f' · 当前 {_text(_fmt(group.get("current_share_yi"), 2, " 亿份"))}</span>'
        )
        details = _table_html(
            etfs,
            [("etf", "包含的 ETF"), ("growth", "份额增幅"), ("change", "份额增减"), ("current", "当前份额")],
        )
        blocks.append(f'<details class="mr-industry"><summary>{summary}</summary>{details}</details>')
    return f'<div class="mr-industry-list">{"".join(blocks)}</div>'


def _focus_html(items: list[dict[str, Any]], evidence_map: dict[str, dict[str, Any]]) -> str:
    if not items:
        return '<div class="mr-empty">暂无通过校验的核心结论</div>'
    cards: list[str] = []
    for index, item in enumerate(items[:3], start=1):
        evidence_ids = [str(value) for value in item.get("evidence_ids") or []]
        labels = [
            evidence_map[evidence_id].get("label")
            for evidence_id in evidence_ids
            if evidence_id in evidence_map and evidence_map[evidence_id].get("label")
        ]
        caveat = str(item.get("caveat") or "").strip()
        caveat_html = f'<p class="mr-focus-caveat">限制：{escape(caveat)}</p>' if caveat else ""
        cards.append(
            '<article class="mr-focus-card">'
            f'<span class="mr-focus-index">0{index}</span>'
            f'<strong>{_text(item.get("text"))}</strong>'
            f'<p>依据：{_text("、".join(str(value) for value in labels if value), "结构化事实")}</p>'
            f"{caveat_html}</article>"
        )
    return f'<div class="mr-focus-grid">{"".join(cards)}</div>'


def _section_header(number: int, title: str, subtitle: str, tone: str) -> str:
    return (
        f'<div class="mr-section-head mr-tone-{tone}">'
        f'<span class="mr-section-number">{number:02d}</span>'
        f'<div><h2>{escape(title)}</h2><p>{escape(subtitle)}</p></div></div>'
    )


def _metric_html(label: str, value: str, note: str, *, tone: str = "neutral") -> str:
    return (
        '<article class="mr-metric">'
        f'<span>{escape(label)}</span><strong class="is-{tone}">{escape(value)}</strong>'
        f'<small>{escape(note)}</small></article>'
    )


def _market_assessment(breadth: dict[str, Any]) -> tuple[str, str, str]:
    advancers = _number(breadth.get("advancer_count"))
    decliners = _number(breadth.get("decliner_count"))
    median = _number(breadth.get("median_pct_chg"))
    if advancers is None or decliners is None or median is None:
        return "数据待补", "neutral", "市场广度数据不足，暂不做强弱定性。"
    spread = advancers - decliners
    if median >= 0.5 and spread > 0:
        label, tone = "偏强", "positive"
    elif median <= -0.5 and spread < 0:
        label, tone = "偏弱", "negative"
    else:
        label, tone = "震荡分化", "warning"
    direction = "上涨家数占优" if spread > 0 else ("下跌家数占优" if spread < 0 else "涨跌家数持平")
    return label, tone, f"{direction}，个股涨跌幅中位数 {_fmt(median, 2, '%', signed=True)}，盘面定性为{label}。"


def _sentiment_assessment(digest: dict[str, Any]) -> tuple[str, str, str]:
    color = str(digest.get("risk_color") or "灰色")
    mapping = {
        "绿色": ("相对稳定", "positive"),
        "黄色": ("分化升温", "warning"),
        "红色": ("谨慎降温", "negative"),
        "灰色": ("数据待补", "neutral"),
    }
    label, tone = mapping.get(color, ("保持观察", "neutral"))
    return label, tone, str(digest.get("risk_text") or "短线情绪数据不足，保持观察。")


def _sorted_growth_groups(groups: list[dict[str, Any]], *, positive: bool) -> list[dict[str, Any]]:
    usable = []
    for group in groups:
        growth = _number(group.get("share_growth_pct"))
        if growth is None or (growth <= 0 if positive else growth >= 0):
            continue
        usable.append((growth, group))
    usable.sort(key=lambda item: item[0], reverse=positive)
    return [group for _, group in usable]


def _sector_lane_html(
    eyebrow: str,
    title: str,
    rows: list[dict[str, Any]],
    *,
    kind: str,
) -> str:
    items: list[str] = []
    for row in rows[:4]:
        if kind == "flow":
            name = row.get("industry") or "未识别板块"
            value = _fmt(row.get("net_amount_yi"), 2, " 亿元", signed=True)
            detail = f"涨跌幅 {_fmt(row.get('pct_change'), 2, '%', signed=True)}"
        else:
            name = row.get("industry") or "未识别行业"
            value = _fmt(row.get("share_growth_pct"), 2, "%", signed=True)
            detail = f"份额增减 {_fmt(row.get('share_change_yi'), 2, ' 亿份', signed=True)}"
        items.append(
            f'<li><span>{_text(name)}</span><strong class="mr-value {_tone_class(row.get("net_amount_yi") if kind == "flow" else row.get("share_growth_pct"))}">{_text(value)}</strong><small>{_text(detail)}</small></li>'
        )
    if not items:
        items.append('<li class="is-empty">本期暂无明确方向</li>')
    return (
        f'<article class="mr-sector-lane mr-lane-{escape(kind)}"><span class="mr-lane-eyebrow">{escape(eyebrow)}</span>'
        f'<h3>{escape(title)}</h3><ul>{"".join(items)}</ul></article>'
    )


def _trend_list_html(items: list[dict[str, Any]], *, cautious: bool = False) -> str:
    if not items:
        return '<div class="mr-empty">暂无模型候选</div>'
    cards: list[str] = []
    for rank, item in enumerate(items[:4], start=1):
        probability = _number(item.get("prob_up_5d"))
        probability_text = "--" if probability is None else f"{probability * 100:.0f}%"
        score_field = "risk_score" if cautious else "trend_score"
        score_label = "风险分" if cautious else "趋势分"
        cards.append(
            '<article class="mr-stock">'
            f'<span class="mr-stock-rank">{rank:02d}</span><div><strong>{_text(item.get("name") or item.get("ts_code"))}</strong>'
            f'<p>{_text(item.get("ts_code"))} · {_text(item.get("industry"), "行业未标注")}</p></div>'
            f'<div class="mr-stock-score"><strong>{_text(_fmt(item.get(score_field), 0))}</strong><small>{score_label}</small></div>'
            f'<span class="mr-prob">5日概率 {_text(probability_text)}</span></article>'
        )
    return f'<div class="mr-stock-list">{"".join(cards)}</div>'


def _detail_block(title: str, subtitle: str, content: str, *, opened: bool = False) -> str:
    open_attr = " open" if opened else ""
    return (
        f'<details class="mr-detail"{open_attr}><summary><div><strong>{escape(title)}</strong>'
        f'<span>{escape(subtitle)}</span></div><span class="mr-detail-action">展开查看</span></summary>'
        f'<div class="mr-detail-body">{content}</div></details>'
    )


def _dashboard_css() -> str:
    return """
<style>
.ws-morning-report{--mr-navy:#0c3656;--mr-blue:#1686c9;--mr-blue-soft:#eaf6fd;--mr-red:#d94b5b;--mr-red-soft:#fff0f2;--mr-orange:#e38823;--mr-orange-soft:#fff6e8;--mr-purple:#7367d8;--mr-purple-soft:#f2f0ff;--mr-green:#18896b;--mr-green-soft:#e9f8f3;--mr-ink:#19394f;--mr-muted:#6f8798;--mr-line:#d7e8f2;display:flex;flex-direction:column;gap:14px;margin:6px 0 28px;color:var(--mr-ink);font-family:var(--ws-font-heading),sans-serif}
.ws-morning-report *{box-sizing:border-box}.mr-hero{position:relative;isolation:isolate;overflow:hidden;padding:28px;border:1px solid rgba(255,255,255,.16);border-radius:22px;background:linear-gradient(122deg,#092f4d 0%,#0d527b 58%,#127da9 100%);box-shadow:0 15px 34px rgba(12,54,86,.20);color:#fff}.mr-hero:before{position:absolute;right:-24px;bottom:-18px;z-index:-1;width:410px;height:210px;background:linear-gradient(155deg,transparent 48%,rgba(114,220,255,.17) 49% 51%,transparent 52%),repeating-linear-gradient(90deg,transparent 0 27px,rgba(255,255,255,.10) 28px 36px);clip-path:polygon(0 72%,9% 57%,18% 63%,27% 34%,36% 48%,46% 18%,55% 29%,65% 5%,75% 27%,84% 0,92% 20%,100% 9%,100% 100%,0 100%);content:""}.mr-hero-top{display:flex;align-items:flex-start;justify-content:space-between;gap:24px}.mr-kicker{display:block;margin-bottom:9px;color:#8edcff;font:700 12px/1.2 var(--ws-font-data);letter-spacing:.14em;text-transform:uppercase}.mr-hero h1{margin:0;color:#fff!important;font-size:30px;line-height:1.15;letter-spacing:-.02em}.mr-hero-copy{max-width:720px;margin:9px 0 0;color:rgba(255,255,255,.78)!important;font-size:15px;line-height:1.6}.mr-hero-date{min-width:150px;padding-left:20px;border-left:1px solid rgba(255,255,255,.23);text-align:right}.mr-hero-date strong{display:block;color:#fff;font:800 24px/1.1 var(--ws-font-data)}.mr-hero-date span{display:block;margin-top:6px;color:rgba(255,255,255,.65);font-size:12px}.mr-meta{display:flex;flex-wrap:wrap;gap:7px;margin-top:19px}.mr-badge{padding:6px 10px;border:1px solid rgba(255,255,255,.16);border-radius:999px;background:rgba(255,255,255,.10);color:#fff;font-size:12px}.mr-badge.is-complete{background:rgba(40,192,135,.22)}.mr-badge.is-partial{background:rgba(255,194,76,.24)}.mr-verdicts{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:20px}.mr-verdict{display:flex;align-items:center;gap:10px;min-height:58px;padding:11px 13px;border:1px solid rgba(255,255,255,.13);border-radius:13px;background:rgba(4,31,51,.28);backdrop-filter:blur(6px)}.mr-verdict span{color:rgba(255,255,255,.62);font-size:12px}.mr-verdict strong{display:block;margin-top:3px;color:#fff;font-size:15px}.mr-verdict-dot{width:7px;height:30px;border-radius:8px;background:#fff}.mr-verdict.is-positive .mr-verdict-dot{background:#55d39d}.mr-verdict.is-warning .mr-verdict-dot{background:#ffc95a}.mr-verdict.is-negative .mr-verdict-dot{background:#ff7581}.mr-verdict.is-neutral .mr-verdict-dot{background:#b9c9d4}
.mr-brief{padding:19px 20px;border:1px solid var(--mr-line);border-radius:18px;background:linear-gradient(135deg,#fff 0%,#f5fbff 100%);box-shadow:0 7px 19px rgba(42,136,192,.08)}.mr-brief-head{display:flex;align-items:center;justify-content:space-between;gap:18px}.mr-brief-label{color:var(--mr-blue);font-size:12px;font-weight:800;letter-spacing:.08em}.mr-brief h2{margin:4px 0 0!important;color:var(--mr-ink)!important;font-size:19px!important}.mr-brief-summary{margin:14px 0 0!important;color:#395a70!important;font-size:15px;line-height:1.75}.mr-focus-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;margin-top:14px}.mr-focus-card{position:relative;min-height:126px;padding:15px;border:1px solid var(--mr-line);border-radius:14px;background:#fff}.mr-focus-index{display:block;margin-bottom:13px;color:var(--mr-blue);font:800 12px/1 var(--ws-font-data)}.mr-focus-card strong{display:block;font-size:14px;line-height:1.55}.mr-focus-card p{margin:9px 0 0!important;color:var(--mr-muted)!important;font-size:12px;line-height:1.45}.mr-focus-caveat{color:#b67818!important}
.mr-section{overflow:hidden;border:1px solid var(--mr-line);border-radius:19px;background:#fff;box-shadow:0 8px 22px rgba(42,136,192,.08)}.mr-section-head{display:flex;align-items:center;gap:13px;padding:14px 18px;border-bottom:1px solid var(--mr-line)}.mr-section-number{display:grid;flex:0 0 36px;height:36px;place-items:center;border-radius:11px;background:var(--mr-blue);color:#fff;font:800 13px/1 var(--ws-font-data)}.mr-section-head h2{margin:0!important;color:var(--mr-ink)!important;font-size:18px!important}.mr-section-head p{margin:3px 0 0!important;color:var(--mr-muted)!important;font-size:12px}.mr-tone-blue{background:linear-gradient(90deg,var(--mr-blue-soft),#fff)}.mr-tone-red{background:linear-gradient(90deg,var(--mr-red-soft),#fff)}.mr-tone-red .mr-section-number{background:var(--mr-red)}.mr-tone-orange{background:linear-gradient(90deg,var(--mr-orange-soft),#fff)}.mr-tone-orange .mr-section-number{background:var(--mr-orange)}.mr-tone-purple{background:linear-gradient(90deg,var(--mr-purple-soft),#fff)}.mr-tone-purple .mr-section-number{background:var(--mr-purple)}.mr-tone-green{background:linear-gradient(90deg,var(--mr-green-soft),#fff)}.mr-tone-green .mr-section-number{background:var(--mr-green)}.mr-section-body{padding:15px 18px 18px}.mr-metrics{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px}.mr-metric{min-height:108px;padding:14px;border:1px solid #e1edf4;border-radius:13px;background:#fbfdff}.mr-metric>span{display:block;color:var(--mr-muted);font-size:12px}.mr-metric>strong{display:block;margin-top:9px;font:800 18px/1.25 var(--ws-font-data)}.mr-metric>small{display:block;margin-top:8px;color:#8aa0ae;font-size:11px;line-height:1.35}.is-positive{color:var(--mr-red)!important}.is-negative{color:var(--mr-green)!important}.is-warning{color:#b67518!important}.is-neutral{color:#537084!important}.mr-conclusion{display:flex;align-items:flex-start;gap:10px;margin-top:11px;padding:11px 13px;border-radius:11px;background:#f1f8fc;color:#31586f;font-size:13px;line-height:1.55}.mr-conclusion:before{content:"结论";flex:0 0 auto;padding:3px 7px;border-radius:6px;background:var(--mr-blue);color:#fff;font-size:11px;font-weight:800}.mr-conclusion.is-red{background:var(--mr-red-soft)}.mr-conclusion.is-red:before{background:var(--mr-red)}.mr-conclusion.is-orange{background:var(--mr-orange-soft)}.mr-conclusion.is-orange:before{background:var(--mr-orange)}
.mr-sector-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.mr-sector-lane{padding:15px;border:1px solid var(--mr-line);border-radius:14px;background:#fbfdff}.mr-lane-eyebrow{color:var(--mr-muted);font-size:11px;font-weight:800;letter-spacing:.08em}.mr-sector-lane h3{margin:5px 0 10px!important;color:var(--mr-ink)!important;font-size:16px!important}.mr-sector-lane ul{display:flex;flex-direction:column;gap:0;margin:0;padding:0;list-style:none}.mr-sector-lane li{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:3px 8px;padding:9px 0;border-top:1px solid #e8f0f5}.mr-sector-lane li>span{font-size:13px;font-weight:700}.mr-sector-lane li>strong{font:800 12px/1.3 var(--ws-font-data)}.mr-sector-lane li>small{grid-column:1/-1;color:var(--mr-muted);font-size:11px}.mr-sector-lane li.is-empty{display:block;color:var(--mr-muted);font-size:13px}.mr-lane-flow{border-top:3px solid var(--mr-red)}.mr-lane-share{border-top:3px solid var(--mr-blue)}.mr-lane-weak{border-top:3px solid var(--mr-green)}
.mr-trend-grid{display:grid;grid-template-columns:1fr 1fr;gap:11px}.mr-trend-panel{padding:15px;border:1px solid var(--mr-line);border-radius:14px;background:#fbfdff}.mr-trend-panel h3{margin:0 0 3px!important;color:var(--mr-ink)!important;font-size:16px!important}.mr-trend-panel>p{margin:0 0 12px!important;color:var(--mr-muted)!important;font-size:12px}.mr-stock-list{display:flex;flex-direction:column;gap:7px}.mr-stock{display:grid;grid-template-columns:28px minmax(0,1fr) auto;align-items:center;gap:8px;padding:10px;border:1px solid #e2edf3;border-radius:11px;background:#fff}.mr-stock-rank{color:#8da4b3;font:700 11px/1 var(--ws-font-data)}.mr-stock>div>strong{font-size:13px}.mr-stock>div>p{margin:3px 0 0!important;color:var(--mr-muted)!important;font-size:11px}.mr-stock-score{text-align:right}.mr-stock-score strong{display:block;color:var(--mr-purple);font:800 15px/1 var(--ws-font-data)}.mr-stock-score small{color:var(--mr-muted);font-size:10px}.mr-prob{grid-column:2/-1;color:#718a9a;font-size:10px}.mr-model-note{margin:11px 0 0!important;color:var(--mr-muted)!important;font-size:11px;line-height:1.5}
.mr-final-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0}.mr-final{min-height:100px;padding:14px 17px;border-right:1px solid var(--mr-line)}.mr-final:last-child{border-right:0}.mr-final span{display:block;color:var(--mr-muted);font-size:12px}.mr-final strong{display:block;margin-top:7px;font-size:18px}.mr-final p{margin:7px 0 0!important;color:#607c8e!important;font-size:12px;line-height:1.45}
.mr-details{display:flex;flex-direction:column;gap:8px}.mr-detail{overflow:hidden;border:1px solid var(--mr-line);border-radius:15px;background:#fff}.mr-detail>summary{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:14px 17px;cursor:pointer;list-style:none}.mr-detail>summary::-webkit-details-marker{display:none}.mr-detail>summary strong{display:block;color:var(--mr-ink);font-size:14px}.mr-detail>summary span{display:block;margin-top:3px;color:var(--mr-muted);font-size:11px}.mr-detail-action{flex:0 0 auto!important;margin:0!important;padding:5px 8px;border-radius:7px;background:#edf7fd;color:var(--mr-blue)!important}.mr-detail[open] .mr-detail-action{font-size:0}.mr-detail[open] .mr-detail-action:after{content:"收起";font-size:11px}.mr-detail-body{padding:0 16px 16px;border-top:1px solid var(--mr-line)}.mr-detail-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding-top:14px}.mr-detail-grid h4{margin:0 0 4px!important;color:var(--mr-ink)!important;font-size:14px!important}.mr-detail-grid p{margin:0 0 9px!important;color:var(--mr-muted)!important;font-size:11px}.mr-table-wrap{overflow:auto}.mr-table-wrap table{width:100%;border-collapse:collapse;font-size:12px}.mr-table-wrap th{background:#eff8fd;color:#52768d;font-weight:700;text-align:left;white-space:nowrap}.mr-table-wrap th,.mr-table-wrap td{padding:9px 10px;border-bottom:1px solid #e2edf3}.mr-table-wrap td{color:#2f5268;white-space:nowrap}.mr-empty{padding:18px;border-radius:11px;background:#f4f9fc;color:var(--mr-muted);font-size:12px;text-align:center}.mr-industry-list{display:flex;flex-direction:column;gap:7px;padding-top:14px}.mr-industry{overflow:hidden;border:1px solid #dfebf2;border-radius:11px}.mr-industry>summary{display:grid;grid-template-columns:minmax(130px,1.2fr) 80px 140px minmax(190px,1fr);align-items:center;gap:9px;padding:11px 13px;cursor:pointer;list-style:none}.mr-industry>summary::-webkit-details-marker{display:none}.mr-industry[open]>summary{background:#f4f9fc}.mr-industry-name{font-size:13px;font-weight:800}.mr-industry-count,.mr-industry-change{color:var(--mr-muted);font-size:11px}.mr-industry .mr-table-wrap{padding:0 11px 9px}.mr-footnote{padding:12px 14px;border:1px dashed #cbdfe9;border-radius:12px;background:#f7fbfd;color:var(--mr-muted);font-size:11px;line-height:1.6}
@media(max-width:1100px){.mr-metrics{grid-template-columns:repeat(3,minmax(0,1fr))}.mr-sector-grid{grid-template-columns:1fr 1fr}.mr-sector-grid .mr-sector-lane:last-child{grid-column:1/-1}.mr-industry>summary{grid-template-columns:1fr 90px 140px}.mr-industry-change{grid-column:1/-1}}
@media(max-width:760px){.mr-hero{padding:21px}.mr-hero-top{display:block}.mr-hero-date{margin-top:18px;padding:12px 0 0;border-top:1px solid rgba(255,255,255,.22);border-left:0;text-align:left}.mr-hero-date strong{font-size:20px}.mr-verdicts,.mr-focus-grid,.mr-trend-grid,.mr-final-grid,.mr-detail-grid{grid-template-columns:1fr}.mr-final{border-right:0;border-bottom:1px solid var(--mr-line)}.mr-final:last-child{border-bottom:0}.mr-metrics{grid-template-columns:1fr 1fr}.mr-sector-grid{grid-template-columns:1fr}.mr-sector-grid .mr-sector-lane:last-child{grid-column:auto}}
@media(max-width:480px){.mr-hero h1{font-size:25px!important}.mr-metrics{grid-template-columns:1fr}.mr-section-head{align-items:flex-start}.mr-section-head p{line-height:1.4}.mr-industry>summary{grid-template-columns:1fr}.mr-industry-change{grid-column:auto}.mr-detail>summary{align-items:flex-start}.mr-detail-action{display:none!important}}
</style>
"""


def build_morning_report_dashboard_html(fact_pack: dict[str, Any], report: dict[str, Any]) -> str:
    """Build the dense, evidence-backed morning review dashboard used by Streamlit."""
    digest = build_report_digest(fact_pack)
    quality = fact_pack.get("data_quality", {}) or {}
    target_date = str(fact_pack.get("report_trade_date") or "")
    generated_at = str(fact_pack.get("generated_at") or report.get("saved_at") or "--")
    generated_display = generated_at.replace("T", " ")[:19]

    overview = fact_pack.get("etf_overview", {}) or {}
    groups = overview.get("industry_etf_groups") or []
    positive_groups = _sorted_growth_groups(groups, positive=True)
    weak_groups = _sorted_growth_groups(groups, positive=False)
    money_flow = fact_pack.get("money_flow", {}) or {}
    ths_rows = money_flow.get("ths_top_inflow") or []
    dc_rows = money_flow.get("dc_top_inflow") or []
    breadth_rows = (fact_pack.get("market_breadth", {}) or {}).get("daily") or []
    sentiment_rows = (fact_pack.get("market_sentiment", {}) or {}).get("limitup") or []
    volume_rows = (fact_pack.get("volume", {}) or {}).get("daily") or []
    north_rows = (fact_pack.get("northbound", {}) or {}).get("daily") or []
    margin_rows = (fact_pack.get("margin", {}) or {}).get("daily") or []
    lhb_rows = (fact_pack.get("dragon_tiger", {}) or {}).get("daily") or []
    breadth = breadth_rows[0] if breadth_rows else {}
    sentiment = sentiment_rows[0] if sentiment_rows else {}
    volume = volume_rows[0] if volume_rows else {}
    north = north_rows[0] if north_rows else {}
    margin = margin_rows[0] if margin_rows else {}
    lhb = lhb_rows[0] if lhb_rows else {}

    evidence_map = {
        str(item.get("evidence_id")): item
        for item in fact_pack.get("evidence") or []
        if item.get("evidence_id")
    }
    llm_analysis = ((report.get("llm") or {}).get("analysis") or {})
    focus_items = llm_analysis.get("focus_items") or digest.get("focus_items") or []
    summary = (llm_analysis.get("summary") or {}).get("text") or ""
    headline = llm_analysis.get("headline") or "上一交易日关键变化与今日观察线索"

    market_label, market_tone, market_note = _market_assessment(breadth)
    sentiment_label, sentiment_tone, sentiment_note = _sentiment_assessment(digest)
    top_sector = str(digest.get("top_sector") or "-")
    share_sector_names = {str(group.get("industry")) for group in positive_groups[:5] if group.get("industry")}
    flow_sector_names = {str(row.get("industry")) for row in ths_rows[:5] if row.get("industry")}
    overlap = [name for name in flow_sector_names if name in share_sector_names]
    if overlap:
        strategy_label = "跟踪共振"
        strategy_note = f"优先验证{overlap[0]}等资金流与 ETF 份额共振方向的持续性。"
    elif top_sector not in {"", "-"}:
        strategy_label = "观察主线"
        strategy_note = f"资金流暂指向{top_sector}，等待 ETF 份额或市场广度进一步确认。"
    else:
        strategy_label = "等待确认"
        strategy_note = "当前缺少清晰资金主线，先观察市场广度与份额变化是否形成共振。"
    if market_tone == "negative":
        strategy_label = "控制节奏"
        strategy_note = f"盘面偏弱，先控制追涨节奏；{strategy_note}"

    status = str(quality.get("report_status") or "partial")
    status_label = "关键数据已齐" if status == "complete" else "部分数据可用"
    status_class = "is-complete" if status == "complete" else "is-partial"
    mode = "证据校验版" if report.get("report_mode") == "llm" else "结构化事实版"

    attempts = (_number(sentiment.get("up_cnt")) or 0) + (_number(sentiment.get("zha_cnt")) or 0)
    blowup_rate = None if attempts <= 0 else (_number(sentiment.get("zha_cnt")) or 0) / attempts * 100
    breadth_date = str(breadth.get("trade_date") or target_date or "--")
    sentiment_note_date = f"数据截至 {target_date or '--'}" if digest.get("sentiment_available") else "该项数据未提供"

    breadth_count_text = (
        f"{int(_number(breadth.get('advancer_count')) or 0)} / {int(_number(breadth.get('decliner_count')) or 0)}"
        if breadth_rows
        else "-- / --"
    )
    market_metrics = "".join([
        _metric_html("上涨 / 下跌", breadth_count_text, f"市场广度 · {breadth_date}", tone=market_tone),
        _metric_html("个股中位数", _fmt(breadth.get("median_pct_chg"), 2, "%", signed=True), "比指数更接近个股体感", tone="positive" if (_number(breadth.get("median_pct_chg")) or 0) > 0 else ("negative" if (_number(breadth.get("median_pct_chg")) or 0) < 0 else "neutral")),
        _metric_html("全市场成交额", _fmt(volume.get("total_amount_yi"), 0, " 亿元"), f"截至 {volume.get('trade_date') or '--'}"),
        _metric_html("北向净流入", _fmt(north.get("north_money_yi"), 2, " 亿元", signed=True), f"截至 {north.get('trade_date') or '--'}", tone="positive" if (_number(north.get("north_money_yi")) or 0) > 0 else ("negative" if (_number(north.get("north_money_yi")) or 0) < 0 else "neutral")),
        _metric_html("融资净买入", _fmt(margin.get("financing_net_buy_yi"), 2, " 亿元", signed=True), f"余额 {_fmt(margin.get('financing_balance_yi'), 0, ' 亿元')}", tone="positive" if (_number(margin.get("financing_net_buy_yi")) or 0) > 0 else ("negative" if (_number(margin.get("financing_net_buy_yi")) or 0) < 0 else "neutral")),
    ])
    sentiment_metrics = "".join([
        _metric_html("涨停", f"{int(_number(sentiment.get('up_cnt')) or 0)} 家" if sentiment_rows else "--", sentiment_note_date, tone="positive"),
        _metric_html("炸板", f"{int(_number(sentiment.get('zha_cnt')) or 0)} 家" if sentiment_rows else "--", "冲板未封住", tone="negative"),
        _metric_html("炸板率", _fmt(blowup_rate, 1, "%"), "炸板 /（涨停 + 炸板）", tone=sentiment_tone),
        _metric_html("强势股", f"{int(_number(breadth.get('strong_advancer_count')) or 0)} 家" if breadth_rows else "--", "涨幅不低于 5%", tone="positive"),
        _metric_html("龙虎榜", f"{int(_number(lhb.get('distinct_stock_count')) or 0)} 家" if lhb_rows else "--", "按证券代码去重"),
    ])

    ths_display = [
        {
            "sector": row.get("industry") or "--",
            "flow": _fmt(row.get("net_amount_yi"), 2, " 亿元", signed=True),
            "change": _fmt(row.get("pct_change"), 2, "%", signed=True),
            "leader": row.get("lead_stock") or "--",
        }
        for row in ths_rows[:10]
    ]
    dc_display = [
        {
            "sector": row.get("industry") or "--",
            "flow": _fmt(row.get("net_amount_yi"), 2, " 亿元", signed=True),
            "change": _fmt(row.get("pct_change"), 2, "%", signed=True),
        }
        for row in dc_rows[:10]
    ]
    funds = (fact_pack.get("fund_watchlist", {}) or {}).get("funds") or []
    fund_rows = [
        {
            "fund": f"{fund.get('fund_name') or '--'}（{fund.get('fund_code') or '--'}）",
            "date": fund.get("nav_date") or "--",
            "change": _fmt(fund.get("daily_change_pct"), 2, "%", signed=True),
        }
        for fund in funds
    ]
    valid_fund_changes = [
        _number(fund.get("daily_change_pct"))
        for fund in funds
        if fund.get("nav_date") == target_date and _number(fund.get("daily_change_pct")) is not None
    ]
    avg_fund_change = sum(valid_fund_changes) / len(valid_fund_changes) if valid_fund_changes else None

    money_flow_tables = (
        '<div class="mr-detail-grid"><div><h4>THS 行业资金流</h4><p>同花顺口径，单位统一为亿元</p>'
        + _table_html(ths_display, [("sector", "行业"), ("flow", "净流入"), ("change", "涨跌幅"), ("leader", "龙头股")])
        + '</div><div><h4>DC 板块资金流</h4><p>东方财富口径，单位统一为亿元</p>'
        + _table_html(dc_display, [("sector", "板块"), ("flow", "净流入"), ("change", "涨跌幅")])
        + "</div></div>"
    )

    summary_html = f'<p class="mr-brief-summary">{escape(str(summary))}</p>' if summary else ""
    risk_note = (llm_analysis.get("risk_note") or {}).get("text") or sentiment_note
    html = f"""{_dashboard_css()}<div class="ws-morning-report">
<header class="mr-hero">
  <div class="mr-hero-top"><div><span class="mr-kicker">A-SHARE · ETF MORNING BRIEF</span><h1>A股 · ETF 晨间复盘</h1><p class="mr-hero-copy">{_text(headline)}</p></div><div class="mr-hero-date"><strong>{_text(target_date)}</strong><span>前一交易日复盘</span></div></div>
  <div class="mr-meta"><span class="mr-badge {status_class}">{status_label}</span><span class="mr-badge">数据覆盖 {int(_number(quality.get('coverage_score')) or 0)}%</span><span class="mr-badge">{mode}</span><span class="mr-badge">生成 {_text(generated_display)}</span></div>
  <div class="mr-verdicts"><div class="mr-verdict is-{market_tone}"><i class="mr-verdict-dot"></i><div><span>盘面</span><strong>{escape(market_label)}</strong></div></div><div class="mr-verdict is-{sentiment_tone}"><i class="mr-verdict-dot"></i><div><span>情绪</span><strong>{escape(sentiment_label)}</strong></div></div><div class="mr-verdict is-warning"><i class="mr-verdict-dot"></i><div><span>策略</span><strong>{escape(strategy_label)}</strong></div></div></div>
</header>
<section class="mr-brief"><div class="mr-brief-head"><div><span class="mr-brief-label">今日先看</span><h2>晨会摘要与关键证据</h2></div></div>{summary_html}{_focus_html(focus_items, evidence_map)}</section>
<section class="mr-section">{_section_header(1, '大盘环境', '先看市场广度、量能与增量资金', 'blue')}<div class="mr-section-body"><div class="mr-metrics">{market_metrics}</div><div class="mr-conclusion">{escape(market_note)}</div></div></section>
<section class="mr-section">{_section_header(2, '市场情绪与赚钱效应', '涨停结构、炸板率与强势股共同判断短线温度', 'red')}<div class="mr-section-body"><div class="mr-metrics">{sentiment_metrics}</div><div class="mr-conclusion is-red">{_text(risk_note)}</div></div></section>
<section class="mr-section">{_section_header(3, '板块复盘', '资金主线、ETF 份额增强与退潮方向分栏观察', 'orange')}<div class="mr-section-body"><div class="mr-sector-grid">{_sector_lane_html('A · 资金主线', '净流入领先', ths_rows, kind='flow')}{_sector_lane_html('B · 份额增强', 'ETF 申购共振', positive_groups, kind='share')}{_sector_lane_html('C · 风险观察', 'ETF 份额走弱', weak_groups, kind='weak')}</div><div class="mr-conclusion is-orange">{escape(strategy_note)}</div></div></section>
<section class="mr-section">{_section_header(4, '核心标的验证', '从趋势模型中找观察对象，并同步保留谨慎清单', 'purple')}<div class="mr-section-body"><div class="mr-trend-grid"><div class="mr-trend-panel"><h3>趋势观察</h3><p>按风险调整后的趋势结果排序</p>{_trend_list_html((fact_pack.get('trend_recommendations', {}) or {}).get('top_uptrend') or [])}</div><div class="mr-trend-panel"><h3>谨慎观察</h3><p>弱趋势、高风险或低概率候选</p>{_trend_list_html((fact_pack.get('trend_recommendations', {}) or {}).get('top_avoid') or [], cautious=True)}</div></div><p class="mr-model-note">模型结果只作为今日验证清单，不等同于实时行情或买卖指令；开盘后仍需观察量价与板块联动。</p></div></section>
<section class="mr-section">{_section_header(5, '今日总判断', '把盘面、情绪和执行重点压缩成三句话', 'green')}<div class="mr-final-grid"><article class="mr-final"><span>大盘</span><strong class="is-{market_tone}">{escape(market_label)}</strong><p>{escape(market_note)}</p></article><article class="mr-final"><span>情绪</span><strong class="is-{sentiment_tone}">{escape(sentiment_label)}</strong><p>{escape(sentiment_note)}</p></article><article class="mr-final"><span>策略</span><strong class="is-warning">{escape(strategy_label)}</strong><p>{escape(strategy_note)}</p></article></div></section>
<div class="mr-details">
{_detail_block('ETF 份额变化明细', '按行业聚合，展开行业可查看所含 ETF', _industry_groups_html(groups), opened=True)}
{_detail_block('行业与板块资金流', 'THS 与 DC 为不同供应商口径，不做绝对值横比', money_flow_tables)}
{_detail_block('自选基金确认净值', f'同报告交易日对齐 {len(valid_fund_changes)} 只，平均 {_fmt(avg_fund_change, 2, "%", signed=True)}', _table_html(fund_rows, [('fund', '基金'), ('date', '净值日期'), ('change', '确认涨跌幅')]))}
{_detail_block('数据源就绪度', f"覆盖率 {int(_number(quality.get('coverage_score')) or 0)}%，缺失数据不会交给模型", _source_table_html(quality.get('sources') or []))}
</div>
<div class="mr-footnote">口径说明：THS 与 DC 资金流虽均换算为亿元，但供应商定义不同，不做绝对值横向比较。北向、两融、成交额按各自上游口径换算；报告用于复盘与观察，不构成投资建议。</div>
</div>"""
    return html


__all__ = ["build_morning_report_dashboard_html"]
