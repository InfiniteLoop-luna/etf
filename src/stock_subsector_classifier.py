from __future__ import annotations

import re
from typing import Iterable


SUBSECTOR_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("MLCC", ("mlcc", "多层陶瓷电容", "片式多层陶瓷电容")),
    ("PCB", ("pcb", "印制电路板", "印刷电路板", "线路板")),
    ("覆铜板", ("覆铜板", "铜箔基板")),
    ("光模块/CPO", ("光模块", "cpo", "硅光模块", "光收发模块")),
    ("光纤光缆", ("光纤", "光缆")),
    ("CMP设备及材料", ("cmp设备", "化学机械抛光", "抛光液", "抛光垫")),
    ("涂胶显影/湿法设备", ("涂胶显影", "涂胶/显影", "湿法设备", "清洗机", "去胶机")),
    ("量测/检测设备", ("量测设备", "量测领域", "缺陷检测", "检测设备", "电子束设备", "膜厚")),
    ("刻蚀设备", ("刻蚀设备", "刻蚀机", "深硅刻蚀")),
    ("薄膜沉积设备", ("薄膜沉积", "pecvd", "sacvd", "mocvd", "原子层沉积", "ald设备")),
    ("光刻设备", ("光刻机", "光刻设备")),
    ("半导体设备零部件", ("半导体设备精密零部件", "半导体设备零部件", "零部件精密制造")),
    ("半导体设备", ("半导体专用设备", "集成电路设备", "半导体设备")),
    ("晶圆制造/代工", ("晶圆制造", "晶圆代工", "开放式晶圆制造")),
    ("封装测试", ("封装测试", "封装、测试", "先进封装")),
    ("模拟芯片", ("模拟集成电路", "模拟芯片")),
    ("功率半导体", ("功率器件", "功率半导体", "igbt", "mosfet")),
    ("存储芯片", ("存储芯片", "dram", "nand flash", "nor flash")),
    ("GPU/AI芯片", ("gpu", "ai芯片", "人工智能芯片")),
    ("连接器", ("连接器", "连接组件")),
    ("消费电子零部件", ("消费电子零部件", "精密结构件", "声学器件")),
    ("锂电池", ("锂离子电池", "动力电池", "储能电池")),
    ("锂电材料", ("正极材料", "负极材料", "电解液", "锂电隔膜")),
    ("光伏设备", ("光伏设备", "硅片设备", "电池片设备")),
    ("机器人", ("工业机器人", "人形机器人", "机器人")),
)


def _normalize_text(values: Iterable[object]) -> str:
    parts = []
    for value in values:
        text = str(value or "").strip().lower()
        if text:
            parts.append(text)
    return re.sub(r"\s+", "", " ".join(parts))


def classify_stock_subsector(
    *,
    industry: object = "",
    main_business: object = "",
    product: object = "",
    introduction: object = "",
) -> dict[str, object]:
    """Return one primary subsector plus all reliable matched labels.

    A single primary label prevents duplicated portfolio weights in treemaps.
    Additional matches are retained as auxiliary tags for hover details.
    """
    text = _normalize_text((main_business, product, introduction))
    matched: list[str] = []
    matched_keywords: list[str] = []
    for label, keywords in SUBSECTOR_RULES:
        hits = [keyword for keyword in keywords if keyword.lower().replace(" ", "") in text]
        if hits:
            matched.append(label)
            matched_keywords.extend(hits)

    industry_label = str(industry or "").strip() or "未识别行业"
    primary = matched[0] if matched else f"其他·{industry_label}"
    return {
        "subsector": primary,
        "subsector_tags": matched,
        "subsector_tag_text": "、".join(matched) if matched else primary,
        "subsector_evidence": "、".join(dict.fromkeys(matched_keywords)),
    }
