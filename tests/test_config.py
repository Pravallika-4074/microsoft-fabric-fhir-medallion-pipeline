from datetime import datetime, timezone
import pytest
from fhir_fabric.config import FhirConfig, incremental_window


def test_config_rejects_non_https():
    with pytest.raises(ValueError):
        FhirConfig(base_url="http://example.com").validate()


def test_incremental_window_is_three_days():
    end = datetime(2026, 1, 4, tzinfo=timezone.utc)
    start, actual_end = incremental_window(3, end)
    assert (actual_end - start).days == 3
