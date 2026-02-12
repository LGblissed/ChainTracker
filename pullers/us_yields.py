"""Puller for U.S. Treasury yields from FRED API."""

import os
from typing import Any, Dict, Tuple

from dotenv import load_dotenv
import requests

from pullers.base_puller import BasePuller


class USYieldsPuller(BasePuller):
    """Pull latest DGS2, DGS10, and DGS30 yields from FRED."""

    SOURCE_URL = "https://api.stlouisfed.org/fred/series/observations"
    SERIES_MAP = {
        "DGS2": "us_2y_yield",
        "DGS10": "us_10y_yield",
        "DGS30": "us_30y_yield",
    }

    def __init__(self):
        """Initialize puller metadata."""
        super().__init__(source_id="fred_us_yields", source_name="FRED U.S. Treasury Yields")

    def _pull_series(self, series_id: str, api_key: str) -> Tuple[float | None, str | None, str, str | None]:
        """Fetch latest available observation for a single FRED series."""
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 10,
        }
        response = None
        try:
            response = requests.get(
                self.SOURCE_URL,
                params=params,
                timeout=25,
                headers={"User-Agent": "ArgentinaChainTracker/1.0"},
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            return None, None, "", f"{series_id} request failed: {exc}"
        except ValueError as exc:
            snippet = response.text[:500] if response is not None else ""
            return None, None, snippet, f"{series_id} invalid JSON response: {exc}"

        snippet = response.text[:500]
        observations = payload.get("observations", [])
        if not observations:
            return None, None, snippet, f"{series_id} observations list is empty"

        for item in observations:
            value_raw = item.get("value")
            if value_raw in (None, "."):
                continue
            try:
                return float(value_raw), item.get("date"), snippet, None
            except ValueError:
                continue

        return None, None, snippet, f"{series_id} has no numeric observation in returned window"

    def pull(self) -> Dict[str, Any]:
        """Pull all configured treasury series in one run."""
        pulled_at = self.utc_now_iso()
        data: Dict[str, Any] = {
            "us_2y_yield": None,
            "us_10y_yield": None,
            "us_30y_yield": None,
            "data_date": None,
        }
        errors: list[str] = []
        snippets: list[str] = []

        project_root = self._project_root()
        load_dotenv(os.path.join(project_root, ".env"))
        api_key = os.getenv("FRED_API_KEY")

        if not api_key:
            return {
                "source_id": self.source_id,
                "pulled_at_utc": pulled_at,
                "status": "error",
                "data": data,
                "errors": ["FRED_API_KEY not configured in .env"],
                "raw_response_snippet": "",
            }

        pulled_dates: list[str] = []
        for series_id, output_key in self.SERIES_MAP.items():
            value, obs_date, snippet, error_msg = self._pull_series(series_id, api_key)
            if snippet:
                snippets.append(snippet)
            data[output_key] = value
            if obs_date:
                pulled_dates.append(obs_date)
            if error_msg:
                errors.append(error_msg)

        if pulled_dates:
            data["data_date"] = max(pulled_dates)

        found_values = sum(1 for key in ["us_2y_yield", "us_10y_yield", "us_30y_yield"] if data[key] is not None)
        if found_values == 3:
            status = "ok"
        elif found_values > 0:
            status = "partial"
        else:
            status = "error"

        return {
            "source_id": self.source_id,
            "pulled_at_utc": pulled_at,
            "status": status,
            "data": data,
            "errors": errors,
            "raw_response_snippet": "\n---\n".join(snippets)[:500],
        }
