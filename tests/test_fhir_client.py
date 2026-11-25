from datetime import datetime, timezone
from unittest.mock import Mock

from fhir_fabric.fhir_client import FhirClient


def test_bundle_resource_extraction():
    from fhir_fabric.record_parser import parse_bundle_entries

    extracted = datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
    bundle = {"entry": [
        {"resource": {"resourceType": "Patient", "id": "p1", "meta": {"lastUpdated": "2026-01-01T00:00:00Z"}}},
        {"resource": {"resourceType": "Observation", "id": "o1"}},
        {"resource": {"resourceType": "Patient", "id": "p2"}},
    ]}
    rows = parse_bundle_entries(bundle, "Patient", extracted, "params", "raw.json", "run-1")
    assert [row["resource_id"] for row in rows] == ["p1", "p2"]
    assert rows[0]["resource_hash"]
    assert rows[0]["source_file"] == "raw.json"


def test_pagination_next_link_host_validation():
    FhirClient._validate_next_url("https://hapi.fhir.org/baseR4?_getpages=x", "https://hapi.fhir.org/baseR4")
