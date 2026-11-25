from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .logging_utils import get_logger


class FhirApiError(RuntimeError):
    """Raised when the FHIR server returns an invalid or unsuccessful response."""


@dataclass
class FhirPage:
    resource_type: str
    page_number: int
    request_url: str
    fetched_at: datetime
    payload: dict[str, Any]


class FhirClient:
    def __init__(self, base_url: str, timeout_seconds: int = 45,
                 max_retries: int = 5, backoff_seconds: float = 1.5) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.backoff_seconds = backoff_seconds
        self.logger = get_logger(f"{__name__}.FhirClient")
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_seconds,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "Accept": "application/fhir+json, application/json",
            "User-Agent": "fabric-fhir-medallion-assignment/1.0",
        })

    @staticmethod
    def _validate_next_url(url: str, base_url: str) -> None:
        candidate, base = urlparse(url), urlparse(base_url)
        if candidate.scheme != "https" or candidate.netloc != base.netloc:
            raise FhirApiError(f"FHIR pagination link points to an untrusted host: {url}")

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            if response.status_code >= 400:
                raise FhirApiError(
                    f"FHIR API returned HTTP {response.status_code}: {response.text[:500]}"
                )
            payload = response.json()
            if not isinstance(payload, dict):
                raise FhirApiError("FHIR response root must be a JSON object")
            return payload
        except requests.RequestException as exc:
            raise FhirApiError(f"FHIR request failed for {url}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise FhirApiError(f"FHIR API returned invalid JSON for {url}") from exc

    def iter_search(self, resource_type: str, start: datetime, end: datetime,
                    page_size: int) -> Iterator[FhirPage]:
        """Follow server-provided Bundle.next links; never calculate page offsets."""
        url = f"{self.base_url}/{resource_type}"
        params = {
            "_count": page_size,
            "_lastUpdated": [f"ge{start.isoformat()}", f"le{end.isoformat()}"],
            "_sort": "_lastUpdated",
        }
        page_number = 0
        while url:
            payload = self._get_json(url, params=params)
            page_number += 1
            yield FhirPage(resource_type, page_number, url, datetime.utcnow(), payload)
            next_url = next((link.get("url") for link in payload.get("link", [])
                             if link.get("relation") == "next"), None)
            if next_url:
                self._validate_next_url(next_url, self.base_url)
            url, params = next_url, None
            if not next_url:
                break
            # Avoid an aggressive request pattern against a shared public endpoint.
            time.sleep(0.1)
