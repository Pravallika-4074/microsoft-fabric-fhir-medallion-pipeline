from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from .config import FhirConfig, incremental_window
from .fhir_client import FhirClient
from .logging_utils import get_logger
from .raw_storage import RawStorage
from .spark_pipeline import SparkMedallionProcessor


class FhirIngestionOrchestrator:
    def __init__(self, spark: Any, config: FhirConfig) -> None:
        config.validate()
        self.config = config
        self.logger = get_logger(f"{__name__}.Orchestrator")
        self.client = FhirClient(config.base_url, config.request_timeout_seconds,
                                 config.max_retries, config.retry_backoff_seconds)
        self.raw = RawStorage(config.raw_root)
        self.processor = SparkMedallionProcessor(
            spark, config.bronze_table_prefix, config.silver_table_prefix, config.audit_table)

    def run(self, lookback_days: int | None = None) -> str:
        run_id = str(uuid.uuid4())
        start, end = incremental_window(lookback_days or self.config.lookback_days)
        self.logger.info("FHIR pipeline started run_id=%s window_start=%s window_end=%s",
                         run_id, start.isoformat(), end.isoformat())
        failures: list[str] = []
        # The ordering is explicit and is also the required orchestration order in the assignment.
        for resource_type in self.config.resources:
            started = datetime.now(timezone.utc).replace(tzinfo=None)
            pages = records_count = 0
            saved_at = None
            error_message = None
            try:
                records: list[dict[str, Any]] = []
                for page in self.client.iter_search(resource_type, start, end, self.config.page_size):
                    pages += 1
                    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    raw_path = self.raw.save_page(page, run_id)
                    records.extend(self.processor.bundle_to_records(
                        page.payload, resource_type, extracted_at,
                        f"_lastUpdated=ge{start.isoformat()}&_count={self.config.page_size}",
                        raw_path, run_id))
                records_count = self.processor.write_bronze(resource_type, records)
                saved_at = datetime.now(timezone.utc).replace(tzinfo=None)
                self.processor.apply_scd2(resource_type)
                status = "SUCCESS"
                self.logger.info("Resource complete resource=%s pages=%s records=%s",
                                 resource_type, pages, records_count)
            except Exception as exc:
                status, error_message = "FAILED", str(exc)[:4000]
                failures.append(f"{resource_type}: {error_message}")
                self.logger.exception("Resource ingestion failed resource=%s run_id=%s",
                                      resource_type, run_id)
            finally:
                completed = datetime.now(timezone.utc).replace(tzinfo=None)
                self.processor.write_audit({
                    "ingestion_run_id": run_id, "resource_type": resource_type,
                    "api_call_started_at": started, "api_call_completed_at": completed,
                    "data_saved_at": saved_at, "request_window_start": start.replace(tzinfo=None),
                    "request_window_end": end.replace(tzinfo=None), "pages_fetched": pages,
                    "records_fetched": records_count, "status": status,
                    "error_message": error_message,
                })
        self.processor.create_gold_views(tuple(r.lower() for r in self.config.resources))
        if failures:
            raise RuntimeError("FHIR pipeline completed with resource failures: " + " | ".join(failures))
        self.logger.info("FHIR pipeline completed successfully run_id=%s", run_id)
        return run_id
