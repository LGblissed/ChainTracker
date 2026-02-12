"""Puller for Argentina FX rates from dolarhoy.com."""

import re
from typing import Any, Dict, Iterable

import requests
from bs4 import BeautifulSoup

from pullers.base_puller import BasePuller


class FXRatesPuller(BasePuller):
    """Pull key ARS FX rates and compute blue vs official spread."""

    SOURCE_URL = "https://dolarhoy.com/"

    def __init__(self):
        """Initialize puller metadata."""
        super().__init__(source_id="fx_rates_dolarhoy", source_name="DolarHoy FX Rates")

    @staticmethod
    def _normalize_text(value: str) -> str:
        """Collapse whitespace and normalize text."""
        return " ".join(value.split()).strip()

    @staticmethod
    def _parse_number(raw_value: str) -> float | None:
        """Parse number tokens with mixed separators into float."""
        cleaned = raw_value.replace("$", "").replace(" ", "").strip()
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

    def _extract_numbers(self, text: str) -> list[float]:
        """Extract all parseable numbers from a text block."""
        numbers: list[float] = []
        for match in re.findall(r"\$?\s*\d[\d\.,]*", text):
            parsed = self._parse_number(match)
            if parsed is not None:
                numbers.append(parsed)
        return numbers

    def _find_card(self, soup: BeautifulSoup, keywords: Iterable[str]) -> BeautifulSoup | None:
        """Find the first card-like element matching any keyword."""
        for tag in soup.find_all(["div", "section", "article", "li", "tr"]):
            text = self._normalize_text(tag.get_text(" ", strip=True)).lower()
            if not text:
                continue
            if any(keyword in text for keyword in keywords):
                return tag
        return None

    def _extract_compra_venta(self, node: BeautifulSoup | None) -> tuple[float | None, float | None]:
        """Extract compra and venta values from a card node."""
        if node is None:
            return None, None

        compra = None
        venta = None
        for child in node.find_all(["div", "span", "p", "td", "strong"]):
            child_text = self._normalize_text(child.get_text(" ", strip=True)).lower()
            numbers = self._extract_numbers(child_text)
            if "compra" in child_text and numbers:
                compra = numbers[0]
            if "venta" in child_text and numbers:
                venta = numbers[0]

        if compra is not None and venta is not None:
            return compra, venta

        fallback_numbers = self._extract_numbers(self._normalize_text(node.get_text(" ", strip=True)))
        if compra is None and len(fallback_numbers) >= 1:
            compra = fallback_numbers[0]
        if venta is None and len(fallback_numbers) >= 2:
            venta = fallback_numbers[1]

        return compra, venta

    def _extract_single(self, node: BeautifulSoup | None) -> float | None:
        """Extract a single representative value from a card node."""
        if node is None:
            return None
        numbers = self._extract_numbers(self._normalize_text(node.get_text(" ", strip=True)))
        if not numbers:
            return None
        return numbers[-1] if len(numbers) > 1 else numbers[0]

    def pull(self) -> Dict[str, Any]:
        """Pull FX rates from DolarHoy and calculate blue spread."""
        pulled_at = self.utc_now_iso()
        data: Dict[str, Any] = {
            "dolar_oficial_compra": None,
            "dolar_oficial_venta": None,
            "dolar_blue_compra": None,
            "dolar_blue_venta": None,
            "dolar_mep": None,
            "dolar_ccl": None,
            "dolar_crypto": None,
            "brecha_blue_vs_oficial_pct": None,
        }
        errors: list[str] = []

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

        html = response.text
        raw_response_snippet = html[:500]
        soup = BeautifulSoup(html, "html.parser")

        oficial_node = self._find_card(soup, ["dolar oficial", "dólar oficial", "oficial"])
        blue_node = self._find_card(soup, ["dolar blue", "dólar blue", "blue"])
        mep_node = self._find_card(soup, ["mep", "bolsa"])
        ccl_node = self._find_card(soup, ["ccl", "contado con liqui", "contado con liquidacion"])
        crypto_node = self._find_card(soup, ["crypto", "cripto"])

        data["dolar_oficial_compra"], data["dolar_oficial_venta"] = self._extract_compra_venta(oficial_node)
        data["dolar_blue_compra"], data["dolar_blue_venta"] = self._extract_compra_venta(blue_node)
        data["dolar_mep"] = self._extract_single(mep_node)
        data["dolar_ccl"] = self._extract_single(ccl_node)
        data["dolar_crypto"] = self._extract_single(crypto_node)

        if data["dolar_oficial_compra"] is None:
            errors.append("dolar_oficial_compra not found on page")
        if data["dolar_oficial_venta"] is None:
            errors.append("dolar_oficial_venta not found on page")
        if data["dolar_blue_compra"] is None:
            errors.append("dolar_blue_compra not found on page")
        if data["dolar_blue_venta"] is None:
            errors.append("dolar_blue_venta not found on page")
        if data["dolar_mep"] is None:
            errors.append("dolar_mep not found on page")
        if data["dolar_ccl"] is None:
            errors.append("dolar_ccl not found on page")
        if data["dolar_crypto"] is None:
            errors.append("dolar_crypto not found on page")

        if data["dolar_blue_venta"] is not None and data["dolar_oficial_venta"] not in (None, 0):
            data["brecha_blue_vs_oficial_pct"] = round(
                ((data["dolar_blue_venta"] / data["dolar_oficial_venta"]) - 1.0) * 100.0,
                2,
            )
        else:
            errors.append("Cannot calculate brecha_blue_vs_oficial_pct due to missing official/blue venta")

        mandatory_fields = [
            "dolar_oficial_compra",
            "dolar_oficial_venta",
            "dolar_blue_compra",
            "dolar_blue_venta",
        ]
        found_mandatory = sum(1 for key in mandatory_fields if data[key] is not None)

        if found_mandatory == len(mandatory_fields):
            status = "ok"
        elif found_mandatory > 0:
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
