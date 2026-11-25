from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any


def parse_bundle_entries(bundle: dict[str, Any], resource_type: str,
                         extracted_at: datetime, api_params: str,
                         source_file: str, run_id: str) -> list[dict[str, Any]]:
    """Extract only the requested FHIR resource type while retaining its complete JSON."""
    records: list[dict[str, Any]] = []
    for entry in bundle.get("entry", []) or []:
        resource = entry.get("resource") or {}
        if resource.get("resourceType") != resource_type or not resource.get("id"):
            continue
        resource_json = json.dumps(resource, sort_keys=True, separators=(",", ":"))
        last_updated = (resource.get("meta") or {}).get("lastUpdated")
        parsed_last_updated = None
        if last_updated:
            try:
                parsed_last_updated = datetime.fromisoformat(last_updated.replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                parsed_last_updated = None
        records.append({
            "resource_type": resource_type,
            "resource_id": str(resource["id"]),
            "last_updated": parsed_last_updated,
            "resource_json": resource_json,
            "resource_hash": hashlib.sha256(resource_json.encode("utf-8")).hexdigest(),
            "extraction_timestamp": extracted_at,
            "api_url_or_params": api_params,
            "source_file": source_file,
            "ingestion_run_id": run_id,
        })
    return records
