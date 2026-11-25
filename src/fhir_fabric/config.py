from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Sequence


@dataclass(frozen=True)
class FhirConfig:
    base_url: str = "https://hapi.fhir.org/baseR4"
    resources: Sequence[str] = ("Patient", "Encounter", "Observation", "Condition")
    page_size: int = 100
    request_timeout_seconds: int = 45
    max_retries: int = 5
    retry_backoff_seconds: float = 1.5
    lookback_days: int = 3
    raw_root: str = "Files/raw/fhir"
    bronze_table_prefix: str = "bronze_fhir_"
    silver_table_prefix: str = "silver_fhir_"
    audit_table: str = "etl_api_audit"
    gold_schema: str = "gold"

    def validate(self) -> None:
        if not self.base_url.startswith("https://"):
            raise ValueError("FHIR base_url must use HTTPS")
        if self.page_size < 1 or self.page_size > 1000:
            raise ValueError("page_size must be between 1 and 1000")
        if self.lookback_days < 1:
            raise ValueError("lookback_days must be >= 1")
        if not self.resources:
            raise ValueError("At least one FHIR resource must be configured")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def incremental_window(lookback_days: int, as_of: datetime | None = None) -> tuple[datetime, datetime]:
    """Return an overlapping UTC window; overlap is intentional for late updates/idempotency."""
    end = as_of or utc_now()
    end = end.astimezone(timezone.utc)
    start = end - timedelta(days=lookback_days)
    return start, end
