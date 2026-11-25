from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from .logging_utils import get_logger


class RawStorage:
    """Writes original API page payloads under the Lakehouse Files area."""
    def __init__(self, root: str = "Files/raw/fhir") -> None:
        self.root = root.rstrip("/")
        self.logger = get_logger(f"{__name__}.RawStorage")

    @staticmethod
    def _safe(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]", "_", value)

    def save_page(self, page: Any, run_id: str) -> str:
        extraction_time = datetime.now(timezone.utc)
        date_bucket = extraction_time.strftime("%Y/%m/%d")
        directory = f"{self.root}/{page.resource_type}/{date_bucket}/run_id={run_id}"
        file_path = f"{directory}/page_{page.page_number:05d}.json"
        try:
            import notebookutils  # Available in Fabric notebooks
            notebookutils.fs.mkdirs(directory)
            # JSON is serialized without flattening or altering the FHIR Bundle structure.
            notebookutils.fs.put(file_path, json.dumps(page.payload, ensure_ascii=False), True)
        except ImportError as exc:
            raise RuntimeError("RawStorage must run inside a Fabric notebook (notebookutils unavailable)") from exc
        self.logger.info("Saved raw FHIR page resource=%s page=%s path=%s",
                         page.resource_type, page.page_number, file_path)
        return file_path
