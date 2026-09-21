from __future__ import annotations

import hashlib
import io
import math
import re
from typing import Any, Callable, Iterable

import pandas as pd
from PIL import Image, ImageEnhance, ImageOps, UnidentifiedImageError


MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_IMAGE_PIXELS = 30_000_000
MAX_BATCH_IMAGE_COUNT = 10
MAX_BATCH_IMAGE_BYTES = 60 * 1024 * 1024
MAX_HOLDING_SHARES = 1_000_000_000_000_000_000.0
SUPPORTED_IMAGE_FORMATS = {"PNG", "JPEG", "WEBP"}

_CODE_PATTERN = re.compile(
    r"(?<![\d.])(\d{6})(?:\s*[.·\-]?\s*(OF|SH|SZ))?(?!\d)(?![.,，]\d)",
    re.IGNORECASE,
)
_OCR_CODE_SEPARATOR_PATTERN = re.compile(
    r"(?<!\d)(\d{6})1(?=\s*(?:混合|股票|债券|指数|货币|基金|QD[I1L]{2}|FOF|高风险|中风险|低风险))",
    re.IGNORECASE,
)
_NUMBER_PATTERN = re.compile(
    r"(?<![\d.])((?:\d{1,3}(?:[,，]\d{3})+|\d+)(?:\.\d+)?)(?:\s*)(亿|万|千)?(?:\s*)(份)?(?!\d)",
    re.IGNORECASE,
)
_SHARE_LABEL_PATTERN = re.compile(r"(?:持有|持仓|可用|基金)?\s*份额|份额\s*\(?份?\)?")
_HOLDING_AMOUNT_LABEL_PATTERN = re.compile(
    r"持有金额|持仓金额|持有市值|持仓市值|当前市值|基金市值|"
    r"^金额\s*[\(（]\s*元\s*[\)）]\s*[0Oo○◯ⓘ。·]?\s*$"
)
_CURRENT_HOLDING_PROFIT_LABEL_PATTERN = re.compile(
    r"持有收益(?!率)|持仓收益(?!率)|"
    r"持仓盈亏(?!率)|浮动盈亏(?!率)|持有盈亏(?!率)"
)
# 累计收益可能包含已经卖出的历史盈亏，只作为版面分隔和槽位识别，
# 绝不能用于反推当前剩余持仓成本。
_CUMULATIVE_PROFIT_LABEL_PATTERN = re.compile(
    r"累计收益(?!率)|累计盈亏(?!率)"
)
_HOLDING_COST_LABEL_PATTERN = re.compile(
    r"持仓成本金额|持有成本金额|成本金额|总成本|累计投入|投入本金|持仓本金"
)
_HOLDING_COST_PRICE_LABEL_PATTERN = re.compile(
    r"持\s*仓\s*成\s*本\s*价|持\s*有\s*成\s*本\s*价"
)
_POSITION_VALUE_LABEL_PATTERNS = (
    _SHARE_LABEL_PATTERN,
    _HOLDING_AMOUNT_LABEL_PATTERN,
    _CURRENT_HOLDING_PROFIT_LABEL_PATTERN,
    _CUMULATIVE_PROFIT_LABEL_PATTERN,
    _HOLDING_COST_LABEL_PATTERN,
    _HOLDING_COST_PRICE_LABEL_PATTERN,
)
_MONEY_VALUE_PATTERN = re.compile(
    r"(?<![\d.])(?:[￥¥]?\s*)?[+\-−]?\s*"
    r"(?:\d{1,3}(?:[,，]\d{3})+|\d+)(?:\.\d+)?"
    r"\s*(?:亿|万|千)?\s*(?:元)?(?![\d.])"
)
_SUMMARY_VALUE_PATTERN = re.compile(
    r"(?<![\d.])(?:[￥¥]?\s*)?[+\-−]?\s*"
    r"(?:\d{1,3}(?:[,，]\d{3})+|\d+)(?:\.\d+)?"
    r"\s*(?:亿|万|千)?\s*(?:元|%)?(?![\d.])"
)
_SUMMARY_LABEL_PATTERN = re.compile(
    r"\d{1,2}\s*(?:月|[-/.])\s*\d{1,2}\s*日?\s*(?:预估|估算)?\s*收益|"
    r"昨日收益|今日收益|当日收益|预估收益|估算收益|"
    r"持有收益率|持仓收益率|累计收益率|"
    r"累计盈亏率|持仓盈亏率|浮动盈亏率|持有盈亏率|"
    r"持有收益(?!率)|持仓收益(?!率)|累计收益(?!率)|"
    r"累计盈亏(?!率)|持仓盈亏(?!率)|浮动盈亏(?!率)|持有盈亏(?!率)"
)
_NON_NAME_PATTERN = re.compile(
    r"持有|持仓|份额|金额|市值|收益|盈亏|净值|成本|资产|代码|基金详情|交易记录|总计|合计"
)


class FundPositionOcrError(RuntimeError):
    """Raised when an uploaded image cannot be safely OCR'd."""


def build_image_batch_fingerprint(images: Iterable[tuple[str, bytes]]) -> str:
    """Build a stable fingerprint for an ordered in-memory screenshot batch."""
    digest = hashlib.sha256()
    image_count = 0
    for index, (name, image_bytes) in enumerate(images):
        image_count += 1
        encoded_name = str(name or "").encode("utf-8", errors="replace")
        payload = bytes(image_bytes or b"")
        digest.update(index.to_bytes(4, "big", signed=False))
        digest.update(len(encoded_name).to_bytes(4, "big", signed=False))
        digest.update(encoded_name)
        digest.update(len(payload).to_bytes(8, "big", signed=False))
        digest.update(payload)
    return digest.hexdigest() if image_count else ""


def _normalize_line(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_fund_code_candidate(value: Any) -> str:
    """Normalize a six-digit OCR code while preserving an explicit suffix."""
    text = str(value or "").strip().upper().replace(" ", "")
    match = re.fullmatch(r"(\d{6})(?:[.·\-]?(OF|SH|SZ))?", text)
    if not match:
        return ""
    code, suffix = match.groups()
    return f"{code}.{suffix}" if suffix else code


def normalize_fund_name_candidate(value: Any) -> str:
    """Build a punctuation-insensitive key for OCR and registry fund names."""
    return "".join(
        character.casefold()
        for character in str(value or "")
        if character.isalnum()
    )


def parse_share_amount(value: Any) -> float | None:
    """Parse a positive number of fund shares, including 万/亿 suffixes."""
    text = str(value or "").strip().replace("，", ",")
    match = re.fullmatch(
        r"((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*(亿|万|千)?\s*(?:份)?",
        text,
    )
    if not match:
        return None
    try:
        number = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    multiplier = {"千": 1_000.0, "万": 10_000.0, "亿": 100_000_000.0}.get(
        match.group(2),
        1.0,
    )
    result = number * multiplier
    return (
        result
        if math.isfinite(result) and 0 < result < MAX_HOLDING_SHARES
        else None
    )


def parse_money_amount(value: Any, *, allow_negative: bool = False) -> float | None:
    """Strictly parse a monetary amount with optional sign and Chinese unit."""
    text = re.sub(r"\s+", "", str(value or "").strip())
    text = text.replace("，", ",").replace("−", "-")
    match = re.fullmatch(
        r"[￥¥]?([+\-]?)"
        r"((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)"
        r"(亿|万|千)?(?:元)?",
        text,
    )
    if not match:
        return None
    try:
        number = float(match.group(2).replace(",", ""))
    except ValueError:
        return None
    if match.group(1) == "-":
        number = -number
    multiplier = {"千": 1_000.0, "万": 10_000.0, "亿": 100_000_000.0}.get(
        match.group(3),
        1.0,
    )
    result = number * multiplier
    if not math.isfinite(result) or abs(result) >= MAX_HOLDING_SHARES:
        return None
    if allow_negative:
        return result
    return result if result > 0 else None


def _prepare_image(image_bytes: bytes) -> Image.Image:
    if not image_bytes:
        raise FundPositionOcrError("上传的截图为空。")
    if len(image_bytes) > MAX_IMAGE_BYTES:
        raise FundPositionOcrError("截图不能超过 12 MB。")
    try:
        source = Image.open(io.BytesIO(image_bytes))
        image_format = str(source.format or "").upper()
        width, height = source.size
        if image_format not in SUPPORTED_IMAGE_FORMATS:
            raise FundPositionOcrError("仅支持 PNG、JPG、JPEG 或 WebP 截图。")
        if width <= 0 or height <= 0 or width * height > MAX_IMAGE_PIXELS:
            raise FundPositionOcrError("截图像素过大，请裁剪后再上传。")
        image = ImageOps.exif_transpose(source).convert("RGB")
    except FundPositionOcrError:
        raise
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise FundPositionOcrError("无法读取截图，请确认图片文件未损坏。") from exc

    # Phone screenshots tend to contain small anti-aliased text.  Upscaling
    # and local contrast make both Tesseract and RapidOCR materially steadier.
    if image.width < 1600:
        pixel_safe_scale = math.sqrt(
            MAX_IMAGE_PIXELS / max(1, image.width * image.height)
        )
        scale = min(2.5, 1600 / max(1, image.width), pixel_safe_scale)
        if scale > 1.0:
            image = image.resize(
                (int(image.width * scale), int(image.height * scale)),
                Image.Resampling.LANCZOS,
            )
    gray = ImageOps.grayscale(image)
    gray = ImageOps.autocontrast(gray, cutoff=1)
    return ImageEnhance.Sharpness(gray).enhance(1.5)


def _group_visual_tokens(tokens: Iterable[dict]) -> list[dict]:
    normalized = []
    for token in tokens:
        text = _normalize_line(token.get("text"))
        if not text:
            continue
        normalized.append(
            {
                "text": text,
                "confidence": float(token.get("confidence") or 0.0),
                "x": float(token.get("x") or 0.0),
                "y": float(token.get("y") or 0.0),
                "height": max(1.0, float(token.get("height") or 1.0)),
            }
        )
    normalized.sort(key=lambda row: (row["y"], row["x"]))

    lines: list[dict] = []
    for token in normalized:
        matching = None
        token_mid = token["y"] + token["height"] / 2.0
        for line in reversed(lines[-4:]):
            tolerance = max(token["height"], line["height"]) * 0.65
            if abs(token_mid - line["mid_y"]) <= tolerance:
                matching = line
                break
        if matching is None:
            lines.append(
                {
                    "tokens": [token],
                    "mid_y": token_mid,
                    "height": token["height"],
                }
            )
        else:
            matching["tokens"].append(token)
            matching["height"] = max(matching["height"], token["height"])
            matching["mid_y"] = sum(
                item["y"] + item["height"] / 2.0 for item in matching["tokens"]
            ) / len(matching["tokens"])

    result = []
    for line in lines:
        line_tokens = sorted(line["tokens"], key=lambda row: row["x"])
        result.append(
            {
                "text": " ".join(row["text"] for row in line_tokens),
                "confidence": round(
                    sum(row["confidence"] for row in line_tokens) / len(line_tokens),
                    4,
                ),
            }
        )
    return result


def _extract_with_tesseract(image: Image.Image) -> list[dict]:
    import pytesseract
    from pytesseract import Output

    last_error: Exception | None = None
    best_lines: list[dict] = []
    for lang in ("chi_sim+eng", "eng"):
        for psm in (6, 11):
            try:
                data = pytesseract.image_to_data(
                    image,
                    lang=lang,
                    config=f"--oem 3 --psm {psm}",
                    output_type=Output.DICT,
                    timeout=20,
                )
            except Exception as exc:  # missing executable/language/timeout
                last_error = exc
                break

            grouped: dict[tuple[int, int, int], list[dict]] = {}
            count = len(data.get("text") or [])
            for index in range(count):
                text = _normalize_line(data["text"][index])
                if not text:
                    continue
                try:
                    confidence = max(0.0, float(data["conf"][index])) / 100.0
                except (TypeError, ValueError):
                    confidence = 0.0
                key = (
                    int(data["block_num"][index]),
                    int(data["par_num"][index]),
                    int(data["line_num"][index]),
                )
                grouped.setdefault(key, []).append(
                    {
                        "text": text,
                        "confidence": confidence,
                        "x": float(data["left"][index]),
                    }
                )
            lines = []
            for tokens in grouped.values():
                tokens.sort(key=lambda row: row["x"])
                lines.append(
                    {
                        "text": " ".join(row["text"] for row in tokens),
                        "confidence": round(
                            sum(row["confidence"] for row in tokens) / len(tokens),
                            4,
                        ),
                    }
                )
            if sum(len(row["text"]) for row in lines) > sum(
                len(row["text"]) for row in best_lines
            ):
                best_lines = lines
        if best_lines:
            return best_lines
    if last_error is not None:
        raise last_error
    return best_lines


def _extract_with_rapidocr(image: Image.Image) -> list[dict]:
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR

    results, _ = RapidOCR()(np.asarray(image.convert("RGB")))
    tokens = []
    for result in results or []:
        if not isinstance(result, (list, tuple)) or len(result) < 3:
            continue
        box, text, confidence = result[:3]
        xs = [float(point[0]) for point in box]
        ys = [float(point[1]) for point in box]
        tokens.append(
            {
                "text": text,
                "confidence": float(confidence or 0.0),
                "x": min(xs),
                "y": min(ys),
                "height": max(ys) - min(ys),
            }
        )
    return _group_visual_tokens(tokens)


def _ocr_result_quality(lines: list[dict]) -> tuple:
    """Rank OCR output by usable position fields, not raw text volume."""
    normalized_lines = [
        row for row in lines if _normalize_line(row.get("text"))
    ]
    text = "\n".join(_normalize_line(row["text"]) for row in normalized_lines)
    parsed_rows = parse_fund_position_text(text, lines=normalized_lines)

    def has_identity(row: dict) -> bool:
        return bool(
            str(row.get("fund_code") or "").strip()
            or str(row.get("fund_name_hint") or "").strip()
        )

    importable_count = sum(
        1
        for row in parsed_rows
        if has_identity(row) and row.get("holding_shares") is not None
    )
    identified_count = sum(1 for row in parsed_rows if has_identity(row))
    cost_count = sum(
        1 for row in parsed_rows if row.get("holding_cost_amount") is not None
    )
    amount_profit_count = sum(
        int(row.get("snapshot_holding_amount") is not None)
        + int(row.get("snapshot_holding_profit") is not None)
        for row in parsed_rows
    )
    financial_field_count = sum(
        int(row.get(field) is not None)
        for row in parsed_rows
        for field in (
            "holding_shares",
            "snapshot_holding_amount",
            "snapshot_holding_profit",
            "holding_cost_amount",
        )
    )
    code_count = sum(
        bool(str(row.get("fund_code") or "").strip()) for row in parsed_rows
    )
    high_confidence_count = sum(
        str(row.get("confidence") or "") == "高" for row in parsed_rows
    )
    confidences = [
        float(row.get("confidence") or 0.0) for row in normalized_lines
    ]
    average_confidence = (
        sum(confidences) / len(confidences) if confidences else 0.0
    )
    return (
        importable_count,
        identified_count,
        cost_count,
        amount_profit_count,
        financial_field_count,
        code_count,
        high_confidence_count,
        average_confidence,
        len(text),
    )


def extract_fund_position_text(image_bytes: bytes) -> dict:
    """OCR an uploaded screenshot in memory and return visual text lines.

    RapidOCR is the primary server-safe engine.  Tesseract is only invoked
    when RapidOCR cannot produce an identified position with valid shares;
    this keeps multi-image batches responsive while retaining a fallback.
    """
    image = _prepare_image(image_bytes)
    errors = []
    successful_results = []
    providers: tuple[tuple[str, Callable[[Image.Image], list[dict]]], ...] = (
        ("RapidOCR", _extract_with_rapidocr),
        ("Tesseract", _extract_with_tesseract),
    )
    for provider_name, extractor in providers:
        try:
            lines = extractor(image)
        except Exception as exc:
            errors.append(f"{provider_name}: {exc}")
            continue
        lines = [row for row in lines if _normalize_line(row.get("text"))]
        if lines:
            result = {
                "provider": provider_name,
                "lines": lines,
                "text": "\n".join(
                    _normalize_line(row["text"]) for row in lines
                ),
                "quality": _ocr_result_quality(lines),
            }
            if provider_name == "RapidOCR" and result["quality"][0] > 0:
                return {
                    "provider": result["provider"],
                    "lines": result["lines"],
                    "text": result["text"],
                    "warnings": errors,
                }
            successful_results.append(result)
        else:
            errors.append(f"{provider_name}: 未识别到文字")
    if successful_results:
        best_result = max(
            successful_results,
            key=lambda result: result["quality"],
        )
        return {
            "provider": best_result["provider"],
            "lines": best_result["lines"],
            "text": best_result["text"],
            "warnings": errors,
        }
    raise FundPositionOcrError(
        "截图识别暂不可用。请改用手工录入，或联系管理员检查 OCR 运行环境。"
    )


def _candidate_numbers(line: str) -> list[dict]:
    results = []
    for match in _NUMBER_PATTERN.finditer(line):
        raw = "".join(part or "" for part in match.groups())
        value = parse_share_amount(raw)
        if value is None:
            continue
        results.append(
            {
                "raw": raw,
                "value": value,
                "has_unit": bool(match.group(2)),
                "has_share_suffix": bool(match.group(3)),
                "start": match.start(),
            }
        )
    return results


def _candidate_money_amounts(line: str, *, allow_negative: bool) -> list[dict]:
    results = []
    for match in _MONEY_VALUE_PATTERN.finditer(line):
        suffix = line[match.end() :].lstrip()
        if suffix.startswith("%"):
            continue
        raw = match.group(0)
        value = parse_money_amount(raw, allow_negative=allow_negative)
        if value is None:
            continue
        results.append({"raw": raw, "value": value, "start": match.start()})
    return results


def _summary_row_values(line: str, *, allow_negative: bool) -> list[dict]:
    """Parse a pure horizontal value row while preserving percentage slots."""
    matches = list(_SUMMARY_VALUE_PATTERN.finditer(line))
    if not matches:
        return []
    remainder_parts = []
    cursor = 0
    for match in matches:
        remainder_parts.append(line[cursor : match.start()])
        cursor = match.end()
    remainder_parts.append(line[cursor:])
    remainder = "".join(remainder_parts)
    if re.sub(r"[\s|｜,，;/；]+", "", remainder):
        return []

    results = []
    for match in matches:
        raw = match.group(0).strip()
        is_percentage = raw.endswith("%")
        value = None
        if not is_percentage:
            value = parse_money_amount(raw, allow_negative=allow_negative)
        results.append(
            {
                "raw": raw,
                "value": value,
                "is_percentage": is_percentage,
            }
        )
    return results


def _aligned_summary_money_candidate(
    label_line: str,
    label_match: re.Match,
    value_line: str,
    *,
    allow_negative: bool,
) -> dict | None:
    """Match horizontal summary labels and values by their left-to-right slot."""
    label_matches = list(_SUMMARY_LABEL_PATTERN.finditer(label_line))
    values = _summary_row_values(value_line, allow_negative=allow_negative)
    if len(label_matches) < 2 or len(label_matches) != len(values):
        return None

    target_index = next(
        (
            index
            for index, match in enumerate(label_matches)
            if match.span() == label_match.span()
        ),
        None,
    )
    if target_index is None:
        return None
    candidate = values[target_index]
    if candidate["is_percentage"] or candidate["value"] is None:
        return None
    return candidate


def _next_position_label_start(line: str, start: int) -> int:
    positions = [
        match.start()
        for pattern in _POSITION_VALUE_LABEL_PATTERNS
        for match in pattern.finditer(line, start)
    ]
    return min(positions) if positions else len(line)


def _line_has_position_value_label(line: str) -> bool:
    return any(pattern.search(line) for pattern in _POSITION_VALUE_LABEL_PATTERNS)


def _best_labeled_money_candidate(
    lines: list[str],
    *,
    label_pattern: re.Pattern,
    code_index: int,
    block_start: int,
    block_end: int,
    allow_negative: bool,
) -> tuple[float | None, str]:
    ranked = []
    for index in range(block_start, block_end):
        line = lines[index]
        summary_label_count = len(list(_SUMMARY_LABEL_PATTERN.finditer(line)))
        for label_match in label_pattern.finditer(line):
            segment_end = _next_position_label_start(line, label_match.end())
            segment = line[label_match.end() : segment_end]
            candidates = _candidate_money_amounts(
                segment,
                allow_negative=allow_negative,
            )
            if candidates:
                candidate = candidates[0]
                score = 120 - abs(index - code_index) * 4
                ranked.append((score, candidate["value"], line))
                continue

            next_index = index + 1
            if next_index < block_end:
                next_line = lines[next_index]
                if not _line_has_position_value_label(next_line):
                    next_candidates = _summary_row_values(
                        next_line,
                        allow_negative=allow_negative,
                    )
                    if (
                        len(next_candidates) == 1
                        and summary_label_count <= 1
                        and not next_candidates[0]["is_percentage"]
                        and next_candidates[0]["value"] is not None
                    ):
                        score = 110 - abs(next_index - code_index) * 4
                        ranked.append(
                            (
                                score,
                                next_candidates[0]["value"],
                                f"{line} | {next_line}",
                            )
                        )
                    else:
                        aligned = _aligned_summary_money_candidate(
                            line,
                            label_match,
                            next_line,
                            allow_negative=allow_negative,
                        )
                        if aligned is not None:
                            score = 108 - abs(next_index - code_index) * 4
                            ranked.append(
                                (
                                    score,
                                    aligned["value"],
                                    f"{line} | {next_line}",
                                )
                            )

            previous_index = index - 1
            if previous_index < block_start or previous_index == code_index:
                continue
            previous_line = lines[previous_index]
            if _line_has_position_value_label(previous_line):
                continue
            previous_values = _summary_row_values(
                previous_line,
                allow_negative=allow_negative,
            )
            if len(previous_values) == 1 and summary_label_count <= 1:
                previous_candidate = previous_values[0]
                if (
                    not previous_candidate["is_percentage"]
                    and previous_candidate["value"] is not None
                ):
                    score = 80 - abs(previous_index - code_index) * 4
                    ranked.append(
                        (
                            score,
                            previous_candidate["value"],
                            f"{previous_line} | {line}",
                        )
                    )
            else:
                aligned = _aligned_summary_money_candidate(
                    line,
                    label_match,
                    previous_line,
                    allow_negative=allow_negative,
                )
                if aligned is not None:
                    score = 78 - abs(previous_index - code_index) * 4
                    ranked.append(
                        (
                            score,
                            aligned["value"],
                            f"{previous_line} | {line}",
                        )
                    )
    if not ranked:
        return None, ""
    ranked.sort(key=lambda row: row[0], reverse=True)
    _, value, evidence = ranked[0]
    return value, evidence


def _name_hint(lines: list[str], code_index: int, code: str = "") -> str:
    if not lines:
        return ""
    start_index = min(max(code_index, 0), len(lines) - 1)
    for index in range(start_index, max(-1, start_index - 8), -1):
        value = lines[index]
        if re.match(r"^\s*\d{6,7}", value) and re.search(
            r"混合|股票|债券|指数|货币|基金|风险|QDII|FOF",
            value,
            re.IGNORECASE,
        ):
            continue
        if code:
            value = _CODE_PATTERN.sub("", value)
        value = re.sub(r"\s*(?:产品详情|基金详情)\s*$", "", value)
        value = re.sub(r"[|｜:：()（）\[\]【】]", " ", value)
        value = _normalize_line(value)
        if (
            2 <= len(value) <= 60
            and re.search(r"[\u4e00-\u9fff]", value)
            and not _NON_NAME_PATTERN.search(value)
        ):
            return value
    return ""


def _best_share_candidate(
    lines: list[str],
    *,
    code: str,
    code_index: int,
    block_start: int,
    block_end: int,
) -> tuple[float | None, int, str]:
    bare_code = code.split(".", 1)[0]
    ranked = []
    for index in range(block_start, block_end):
        line = lines[index]
        share_match = _SHARE_LABEL_PATTERN.search(line)
        previous_has_share_label = (
            index > block_start
            and bool(_SHARE_LABEL_PATTERN.search(lines[index - 1]))
            and not _line_has_position_value_label(line)
        )
        if share_match:
            segment_end = _next_position_label_start(line, share_match.end())
            candidate_numbers = _candidate_numbers(
                line[share_match.end() : segment_end]
            )
            context_score = 100
        elif previous_has_share_label:
            candidate_numbers = _candidate_numbers(line)
            context_score = 80
        else:
            candidate_numbers = [
                number
                for number in _candidate_numbers(line)
                if number["has_share_suffix"]
            ]
            context_score = 45
        for number in candidate_numbers:
            normalized_digits = re.sub(r"\D", "", number["raw"])
            if normalized_digits == bare_code:
                continue
            score = max(0, 24 - abs(index - code_index) * 4) + context_score
            if number["has_share_suffix"]:
                score += 45
            if number["has_unit"]:
                score += 20
            if index == code_index:
                score += 12
            if re.search(r"20\d{2}[-/.年]", line):
                score -= 100
            ranked.append((score, -abs(index - code_index), number["value"], line))
    if not ranked:
        return None, 0, ""
    ranked.sort(reverse=True)
    score, _, value, evidence = ranked[0]
    if score < 12:
        return None, score, evidence
    return value, score, evidence


def _extract_position_financials(
    lines: list[str],
    *,
    holding_shares: float | None,
    code_index: int,
    block_start: int,
    block_end: int,
) -> dict:
    snapshot_amount, amount_evidence = _best_labeled_money_candidate(
        lines,
        label_pattern=_HOLDING_AMOUNT_LABEL_PATTERN,
        code_index=code_index,
        block_start=block_start,
        block_end=block_end,
        allow_negative=False,
    )
    snapshot_profit, profit_evidence = _best_labeled_money_candidate(
        lines,
        label_pattern=_CURRENT_HOLDING_PROFIT_LABEL_PATTERN,
        code_index=code_index,
        block_start=block_start,
        block_end=block_end,
        allow_negative=True,
    )
    explicit_cost, cost_evidence = _best_labeled_money_candidate(
        lines,
        label_pattern=_HOLDING_COST_LABEL_PATTERN,
        code_index=code_index,
        block_start=block_start,
        block_end=block_end,
        allow_negative=False,
    )
    cost_price, cost_price_evidence = _best_labeled_money_candidate(
        lines,
        label_pattern=_HOLDING_COST_PRICE_LABEL_PATTERN,
        code_index=code_index,
        block_start=block_start,
        block_end=block_end,
        allow_negative=False,
    )

    derived_cost = None
    cost_source = ""
    cost_warning = ""
    cost_price_cost = None
    if cost_price is not None and holding_shares is not None:
        candidate_cost = cost_price * holding_shares
        if math.isfinite(candidate_cost) and 0 < candidate_cost < MAX_HOLDING_SHARES:
            cost_price_cost = candidate_cost

    amount_profit_cost = None
    if snapshot_amount is not None and snapshot_profit is not None:
        candidate_cost = snapshot_amount - snapshot_profit
        if math.isfinite(candidate_cost) and 0 < candidate_cost < MAX_HOLDING_SHARES:
            amount_profit_cost = candidate_cost

    if cost_price_cost is not None:
        derived_cost = cost_price_cost
        cost_source = "截图持仓成本价×持有份额"
        if explicit_cost is not None:
            tolerance = max(2.0, abs(explicit_cost) * 0.005)
            if abs(explicit_cost - cost_price_cost) > tolerance:
                cost_warning = "截图成本价计算结果与明确成本金额不一致，请人工核对"
    elif explicit_cost is not None:
        derived_cost = explicit_cost
        cost_source = "截图明确成本金额"
        if amount_profit_cost is not None:
            tolerance = max(2.0, abs(explicit_cost) * 0.005)
            if abs(explicit_cost - amount_profit_cost) > tolerance:
                cost_warning = "截图成本与持有金额、持有收益不一致，请人工核对"
    elif amount_profit_cost is not None:
        derived_cost = amount_profit_cost
        cost_source = "截图持有金额－持有收益"

    return {
        "snapshot_holding_amount": snapshot_amount,
        "snapshot_holding_profit": snapshot_profit,
        "holding_cost_amount": derived_cost,
        "holding_cost_source": cost_source,
        "holding_cost_warning": cost_warning,
        "financial_evidence": " | ".join(
            value
            for value in [
                amount_evidence,
                profit_evidence,
                cost_evidence,
                cost_price_evidence,
            ]
            if value
        )[:360],
    }


def _confidence_label(score: int, line_confidence: float | None = None) -> str:
    confidence = float(line_confidence or 0.0)
    if score >= 100 and confidence >= 0.55:
        return "高"
    if score >= 70 or confidence >= 0.75:
        return "中"
    return "低"


def parse_fund_position_text(text: str, *, lines: list[dict] | None = None) -> list[dict]:
    """Extract reviewable fund-code/share candidates from OCR text.

    The result is intentionally advisory.  It retains low-confidence or
    share-missing rows so the UI can ask the user to correct them before any
    database write.
    """
    if lines:
        text_lines = [_normalize_line(row.get("text")) for row in lines]
        line_confidences = [float(row.get("confidence") or 0.0) for row in lines]
    else:
        text_lines = [_normalize_line(row) for row in str(text or "").splitlines()]
        line_confidences = [0.0] * len(text_lines)
    filtered = [
        (line, line_confidences[index])
        for index, line in enumerate(text_lines)
        if line
    ]
    text_lines = [row[0] for row in filtered]
    line_confidences = [row[1] for row in filtered]
    if not text_lines:
        return []

    code_entries = []
    for index, line in enumerate(text_lines):
        code_scan_line = _OCR_CODE_SEPARATOR_PATTERN.sub(r"\1|", line)
        for match in _CODE_PATTERN.finditer(code_scan_line):
            trailing = code_scan_line[match.end() :].lstrip()
            label_before_code = any(
                label_match.start() < match.start()
                for pattern in _POSITION_VALUE_LABEL_PATTERNS
                for label_match in pattern.finditer(code_scan_line)
            )
            explicit_code_label = bool(
                re.search(
                    r"(?:基金)?代码\s*[:：]?\s*$",
                    code_scan_line[: match.start()],
                )
            )
            numeric_only_after_value_label = (
                index > 0
                and _line_has_position_value_label(text_lines[index - 1])
                and bool(
                    re.fullmatch(
                        r"[￥¥+\-−\d\s,.，万亿千份元%]+",
                        line,
                    )
                )
            )
            if (
                (label_before_code and not explicit_code_label)
                or trailing.startswith(("份", "万", "亿", "千", "元", "%"))
                or numeric_only_after_value_label
            ):
                continue
            raw_code = match.group(1) + (f".{match.group(2)}" if match.group(2) else "")
            code = normalize_fund_code_candidate(raw_code)
            if code:
                code_entries.append((index, code))

    candidates = []
    seen_codes = set()
    for position, (index, code) in enumerate(code_entries):
        if code in seen_codes:
            continue
        seen_codes.add(code)
        previous_index = code_entries[position - 1][0] if position > 0 else -1
        next_index = (
            code_entries[position + 1][0]
            if position + 1 < len(code_entries)
            else len(text_lines)
        )
        block_start = max(previous_index + 1, index - 2, 0)
        block_end = max(index + 1, next_index)
        holding_shares, score, share_evidence = _best_share_candidate(
            text_lines,
            code=code,
            code_index=index,
            block_start=block_start,
            block_end=block_end,
        )
        confidence = _confidence_label(score, line_confidences[index])
        financials = _extract_position_financials(
            text_lines,
            holding_shares=holding_shares,
            code_index=index,
            block_start=block_start,
            block_end=block_end,
        )
        warnings = []
        if holding_shares is None:
            warnings.append("未可靠识别持有份额，请手工补充")
        if financials["holding_cost_warning"]:
            warnings.append(financials["holding_cost_warning"])
        candidates.append(
            {
                "fund_code": code,
                "fund_name_hint": _name_hint(text_lines, index, code),
                "holding_shares": holding_shares,
                **financials,
                "confidence": confidence,
                "evidence": " | ".join(
                    value
                    for value in [text_lines[index], share_evidence]
                    if value
                )[:240],
                "warning": "；".join(warnings),
            }
        )

    if candidates:
        return candidates

    # Some apps omit the fund code and show only the name.  Preserve these as
    # name hints; the UI will accept them only when the fund registry yields a
    # unique match.
    for index, line in enumerate(text_lines):
        if not _SHARE_LABEL_PATTERN.search(line):
            continue
        holding_shares, score, share_evidence = _best_share_candidate(
            text_lines,
            code="",
            code_index=index,
            block_start=max(0, index - 1),
            block_end=min(len(text_lines), index + 3),
        )
        name_hint = _name_hint(text_lines, index - 1)
        if name_hint or holding_shares is not None:
            block_start = max(0, index - 6)
            block_end = min(len(text_lines), index + 5)
            financials = _extract_position_financials(
                text_lines,
                holding_shares=holding_shares,
                code_index=index,
                block_start=block_start,
                block_end=block_end,
            )
            warning_values = ["需通过基金名称确认代码"]
            if financials["holding_cost_warning"]:
                warning_values.append(financials["holding_cost_warning"])
            candidates.append(
                {
                    "fund_code": "",
                    "fund_name_hint": name_hint,
                    "holding_shares": holding_shares,
                    **financials,
                    "confidence": _confidence_label(score, line_confidences[index]),
                    "evidence": " | ".join(
                        value for value in [name_hint, line, share_evidence] if value
                    )[:240],
                    "warning": "；".join(warning_values),
                }
            )
    return candidates


def choose_unique_fund_match(query: str, matches: pd.DataFrame | None) -> dict | None:
    """Choose a registry row only when code/name resolution is unambiguous."""
    if matches is None or matches.empty:
        return None
    frame = matches.copy()
    if "fund_code" not in frame.columns:
        return None
    frame["fund_code"] = frame["fund_code"].astype(str).str.strip().str.upper()
    normalized_query = normalize_fund_code_candidate(query)
    if normalized_query:
        if "." in normalized_query:
            exact = frame[frame["fund_code"] == normalized_query]
        else:
            exact = frame[
                frame["fund_code"].str.split(".", regex=False).str[0]
                == normalized_query
            ]
        if len(exact) == 1:
            return exact.iloc[0].to_dict()
        return None

    normalized_name = normalize_fund_name_candidate(query)
    if not normalized_name or "name" not in frame.columns:
        return None
    exact_name = frame[
        frame["name"].map(normalize_fund_name_candidate) == normalized_name
    ]
    if len(exact_name) == 1:
        return exact_name.iloc[0].to_dict()
    return None


__all__ = [
    "FundPositionOcrError",
    "MAX_BATCH_IMAGE_BYTES",
    "MAX_BATCH_IMAGE_COUNT",
    "MAX_HOLDING_SHARES",
    "MAX_IMAGE_BYTES",
    "build_image_batch_fingerprint",
    "choose_unique_fund_match",
    "extract_fund_position_text",
    "normalize_fund_code_candidate",
    "normalize_fund_name_candidate",
    "parse_money_amount",
    "parse_fund_position_text",
    "parse_share_amount",
]
