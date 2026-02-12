"""Puller for BCRA reserves and monetary base from Principales Variables."""

from datetime import datetime
import re
from typing import Any, Dict, Iterable

import requests
from bs4 import BeautifulSoup

from pullers.base_puller import BasePuller


class BCRAReservesPuller(BasePuller):
    """Pull BCRA reserves and base monetaria from official table."""

    SOURCE_URL = "https://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_datos.asp"

    def __init__(self):
        """Initialize puller metadata."""
        super().__init__(source_id="bcra_reserves", source_name="BCRA International Reserves")

    @staticmethod
    def _normalize_text(value: str) -> str:
        """Collapse whitespace and trim a text fragment."""
        return " ".join(value.split()).strip()

    @staticmethod
    def _parse_number(raw_value: str) -> float | None:
        """Parse numeric strings that may use Argentine separators."""
        cleaned = raw_value.replace("$", "").replace("%", "").replace(" ", "").strip()
        if not cleaned:
            return None

        if "," in cleaned and "." in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        else:
            parts = cleaned.split(".")
            if len(parts) > 1 and all(len(part) == 3 for part in parts[1:]):
                cleaned = "".join(parts)

        try:
            return float(cleaned)
        except ValueError:
            return None

    def _extract_first_number(self, cells: Iterable[str]) -> float | None:
        """Extract first parseable number from row cells."""
        pattern = re.compile(r"[-+]?\d[\d\.,]*")
        for cell in cells:
            for match in pattern.findall(cell):
                parsed = self._parse_number(match)
                if parsed is not None:
                    return parsed
        return None

    @staticmethod
    def _extract_date(text: str) -> str | None:
        """Extract a normalized YYYY-MM-DD date from a text fragment."""
        candidates = re.findall(r"\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}", text)
        for match in candidates:
            try:
                return datetime.strptime(match, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
            try:
                return datetime.strptime(match, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None

    def pull(self) -> Dict[str, Any]:
        """Pull and parse reserves + base monetaria from BCRA page."""
        pulled_at = self.utc_now_iso()
        errors: list[str] = []
        data: Dict[str, Any] = {
            "reservas_internacionales_usd_mm": None,
            "base_monetaria_ars_mm": None,
            "data_date": None,
        }

        try:
            response = requests.get(
                self.SOURCE_URL,
                timeout=25,
                headers={"User-Agent": "ArgentinaChainTracker/1.0"},
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            return {
                "source_id": self.source_id,
                "pulled_at_utc": pulled_at,
                "status": "error",
                "data": data,
                "errors": [f"Request failed: {exc}"],
                "raw_response_snippet": "",
            }

        response.encoding = response.apparent_encoding or response.encoding
        html = response.text
        raw_response_snippet = html[:500]

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all("tr")
        if not rows:
            return {
                "source_id": self.source_id,
                "pulled_at_utc": pulled_at,
                "status": "error",
                "data": data,
                "errors": ["No table rows found. Page structure may have changed or requires JS rendering."],
                "raw_response_snippet": raw_response_snippet,
            }

        for row in rows:
            cells = [self._normalize_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
            if not cells:
                continue

            row_text = " | ".join(cells)
            row_text_lower = row_text.lower()

            if data["reservas_internacionales_usd_mm"] is None and "reservas internacionales" in row_text_lower:
                data["reservas_internacionales_usd_mm"] = self._extract_first_number(cells[1:] or cells)
                data["data_date"] = data["data_date"] or self._extract_date(row_text)

            if data["base_monetaria_ars_mm"] is None and "base monetaria" in row_text_lower:
                data["base_monetaria_ars_mm"] = self._extract_first_number(cells[1:] or cells)
                data["data_date"] = data["data_date"] or self._extract_date(row_text)

            if all(value is not None for key, value in data.items() if key != "data_date"):
                break

        if data["reservas_internacionales_usd_mm"] is None:
            errors.append("Reservas Internacionales row not found or unparseable.")
        if data["base_monetaria_ars_mm"] is None:
            errors.append("Base Monetaria row not found or unparseable.")
        if data["data_date"] is None:
            errors.append("Data date not found in parsed rows.")

        found_core_fields = sum(
            1
            for field_name in ["reservas_internacionales_usd_mm", "base_monetaria_ars_mm"]
            if data[field_name] is not None
        )
        if found_core_fields == 2:
            status = "ok" if len(errors) <= 1 else "partial"
        elif found_core_fields == 1:
            status = "partial"
        else:
            status = "error"

        return {
            "source_id": self.source_id,
            "pulled_at_utc": pulled_at,
            "status": status,
            "data": data,
            "errors": errors,
            "raw_response_snippet": raw_response_snippet,
        }
