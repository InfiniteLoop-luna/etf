from __future__ import annotations

import io
import math
import re
from typing import Any, Callable, Iterable

import pandas as pd
from PIL import Image, ImageEnhance, ImageOps, UnidentifiedImageError


MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_IMAGE_PIXELS = 30_000_000
MAX_HOLDING_SHARES = 1_000_000_000_000_000_000.0
SUPPORTED_IMAGE_FORMATS = {"PNG", "JPEG", "WEBP"}

_CODE_PATTERN = re.compile(
    r"(?<![\d.])(\d{6})(?:\s*[.·\-]?\s*(OF|SH|SZ))?(?!\d)(?![.,，]\d)",
    re.IGNORECASE,
)
_NUMBER_PATTERN = re.compile(
    r"(?<![\d.])((?:\d{1,3}(?:[,，]\d{3})+|\d+)(?:\.\d+)?)(?:\s*)(亿|万|千)?(?:\s*)(份)?(?!\d)",
    re.IGNORECASE,
)
_SHARE_LABEL_PATTERN = re.compile(r"(?:持有|持仓|可用|基金)?\s*份额|份额\s*\(?份?\)?")
_MONEY_LABEL_PATTERN = re.compile(
    r"持有金额|持仓金额|金额|市值|收益|盈亏|净值|成本|资产|本金|昨日收益|累计收益"
)
_NON_NAME_PATTERN = re.compile(
    r"持有|持仓|份额|金额|市值|收益|盈亏|净值|成本|资产|代码|基金详情|交易记录|总计|合计"
)


class FundPositionOcrError(RuntimeError):
    """Raised when an uploaded image cannot be safely OCR'd."""


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


def extract_fund_position_text(image_bytes: bytes) -> dict:
    """OCR an uploaded screenshot in memory and return visual text lines."""
    image = _prepare_image(image_bytes)
    errors = []
    providers: tuple[tuple[str, Callable[[Image.Image], list[dict]]], ...] = (
        ("Tesseract", _extract_with_tesseract),
        ("RapidOCR", _extract_with_rapidocr),
    )
    for provider_name, extractor in providers:
        try:
            lines = extractor(image)
        except Exception as exc:
            errors.append(f"{provider_name}: {exc}")
            continue
        lines = [row for row in lines if _normalize_line(row.get("text"))]
        if lines:
            return {
                "provider": provider_name,
                "lines": lines,
                "text": "\n".join(_normalize_line(row["text"]) for row in lines),
                "warnings": errors,
            }
        errors.append(f"{provider_name}: 未识别到文字")
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


def _name_hint(lines: list[str], code_index: int, code: str = "") -> str:
    if not lines:
        return ""
    start_index = min(max(code_index, 0), len(lines) - 1)
    for index in range(start_index, max(-1, start_index - 4), -1):
        value = lines[index]
        if code:
            value = _CODE_PATTERN.sub("", value)
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
        line_has_share_label = bool(_SHARE_LABEL_PATTERN.search(line))
        line_has_money_label = bool(_MONEY_LABEL_PATTERN.search(line))
        previous_has_share_label = index > 0 and bool(
            _SHARE_LABEL_PATTERN.search(lines[index - 1])
        )
        for number in _candidate_numbers(line):
            normalized_digits = re.sub(r"\D", "", number["raw"])
            if normalized_digits == bare_code:
                continue
            if not (
                line_has_share_label
                or previous_has_share_label
                or number["has_share_suffix"]
            ):
                continue
            score = max(0, 24 - abs(index - code_index) * 4)
            if line_has_share_label:
                score += 100
            elif previous_has_share_label:
                score += 80
            if number["has_share_suffix"]:
                score += 45
            if number["has_unit"]:
                score += 20
            if index == code_index:
                score += 12
            if line_has_money_label and not line_has_share_label:
                score -= 160
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
        for match in _CODE_PATTERN.finditer(line):
            share_label = _SHARE_LABEL_PATTERN.search(line)
            trailing = line[match.end() :].lstrip()
            numeric_only_after_share_label = (
                index > 0
                and _SHARE_LABEL_PATTERN.search(text_lines[index - 1])
                and bool(re.fullmatch(r"[\d\s,.，万亿千份]+", line))
            )
            if (
                (share_label and match.start() > share_label.start())
                or trailing.startswith(("份", "万", "亿", "千", "元", "%"))
                or numeric_only_after_share_label
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
        candidates.append(
            {
                "fund_code": code,
                "fund_name_hint": _name_hint(text_lines, index, code),
                "holding_shares": holding_shares,
                "confidence": confidence,
                "evidence": " | ".join(
                    value
                    for value in [text_lines[index], share_evidence]
                    if value
                )[:240],
                "warning": "" if holding_shares is not None else "未可靠识别持有份额，请手工补充",
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
            candidates.append(
                {
                    "fund_code": "",
                    "fund_name_hint": name_hint,
                    "holding_shares": holding_shares,
                    "confidence": _confidence_label(score, line_confidences[index]),
                    "evidence": " | ".join(
                        value for value in [name_hint, line, share_evidence] if value
                    )[:240],
                    "warning": "需通过基金名称确认代码",
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

    normalized_name = re.sub(r"\s+", "", str(query or "")).casefold()
    if not normalized_name or "name" not in frame.columns:
        return None
    exact_name = frame[
        frame["name"].astype(str).str.replace(r"\s+", "", regex=True).str.casefold()
        == normalized_name
    ]
    if len(exact_name) == 1:
        return exact_name.iloc[0].to_dict()
    return None


__all__ = [
    "FundPositionOcrError",
    "MAX_HOLDING_SHARES",
    "MAX_IMAGE_BYTES",
    "choose_unique_fund_match",
    "extract_fund_position_text",
    "normalize_fund_code_candidate",
    "parse_fund_position_text",
    "parse_share_amount",
]
