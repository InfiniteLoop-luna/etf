from __future__ import annotations

from typing import Any

import requests


class NullOcrProvider:
    provider_name = "none"

    def extract_post_images(self, post: dict[str, Any]) -> list[dict[str, Any]]:
        records = []
        for index, image_url in enumerate(post.get("post_pic_url") or []):
            records.append(
                {
                    "image_index": index,
                    "image_url": image_url,
                    "ocr_status": "skipped",
                    "ocr_text": "",
                    "ocr_provider": self.provider_name,
                }
            )
        return records


class OptionalTesseractOcrProvider:
    provider_name = "tesseract"

    def __init__(self, session: requests.Session | None = None, timeout: int = 15):
        self._session = session or requests.Session()
        self._timeout = int(timeout)

    def _extract_text_from_bytes(self, image_bytes: bytes) -> tuple[str, str]:
        try:
            from PIL import Image
            import pytesseract
        except Exception:
            return "unavailable", ""

        try:
            import io

            image = Image.open(io.BytesIO(image_bytes))
            return "ok", pytesseract.image_to_string(image, lang="chi_sim+eng")
        except Exception:
            return "error", ""

    def extract_post_images(self, post: dict[str, Any]) -> list[dict[str, Any]]:
        records = []
        for index, image_url in enumerate(post.get("post_pic_url") or []):
            try:
                response = self._session.get(image_url, timeout=self._timeout)
                response.raise_for_status()
                status, text_value = self._extract_text_from_bytes(response.content)
            except Exception:
                status, text_value = "error", ""
            records.append(
                {
                    "image_index": index,
                    "image_url": image_url,
                    "ocr_status": status,
                    "ocr_text": text_value,
                    "ocr_provider": self.provider_name,
                }
            )
        return records
