"""Build a Spanish research digest from local DeepResearch files.

Inputs expected in workspace root:
- DeepResearch.pdf
- Argentina+CryptoAnalisis.docx

Output:
- argentina-chain-tracker/config/research_digest.json
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import zipfile
import xml.etree.ElementTree as et

from PyPDF2 import PdfReader


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_line(value: str) -> str:
    cleaned = value.replace("\x00", "").strip()
    cleaned = _fix_mojibake(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _fix_mojibake(value: str) -> str:
    """Best-effort fix for common UTF-8/Latin-1 mojibake patterns."""
    if not value:
        return value
    suspects = ("Ã", "â", "ðŸ", "Â")
    if not any(mark in value for mark in suspects):
        return value
    for encoding in ("latin-1", "cp1252"):
        try:
            repaired = value.encode(encoding).decode("utf-8")
            if repaired.count("�") <= value.count("�"):
                return repaired
        except UnicodeError:
            continue
    return value


def _extract_pdf_lines(path: Path) -> list[str]:
    reader = PdfReader(str(path))
    lines: list[str] = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if line:
                lines.append(line)
    return lines


def _extract_docx_paragraphs(path: Path) -> list[str]:
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    with zipfile.ZipFile(path, "r") as archive:
        xml_bytes = archive.read("word/document.xml")
    root = et.fromstring(xml_bytes)

    paragraphs: list[str] = []
    for paragraph in root.findall(".//w:p", ns):
        text_fragments = [(node.text or "") for node in paragraph.findall(".//w:t", ns)]
        raw = "".join(text_fragments)
        line = _clean_line(raw)
        if line:
            paragraphs.append(line)
    return paragraphs


def _collect_section_block(paragraphs: list[str], title: str, stop_titles: set[str], max_items: int = 10) -> list[str]:
    start_index = None
    for idx, value in enumerate(paragraphs):
        if value.strip() == title:
            start_index = idx + 1
            break
    if start_index is None:
        return []

    block: list[str] = []
    for value in paragraphs[start_index:]:
        if value in stop_titles:
            break
        if re.match(r"^[A-Z]\.\s", value):
            break
        if len(value) >= 8:
            block.append(value)
        if len(block) >= max_items:
            break
    return block


def build_digest(project_root: Path):
    workspace_root = project_root.parent
    pdf_path = workspace_root / "DeepResearch.pdf"
    docx_path = workspace_root / "Argentina+CryptoAnalisis.docx"

    if not pdf_path.exists():
        raise FileNotFoundError(f"Missing PDF: {pdf_path}")
    if not docx_path.exists():
        raise FileNotFoundError(f"Missing DOCX: {docx_path}")

    pdf_lines = _extract_pdf_lines(pdf_path)
    doc_paragraphs = _extract_docx_paragraphs(docx_path)

    stop_titles = {
        "B. METHODOLOGY + DATA SOURCES",
        "C. PART 1 — ARGENTINA MACRO REGIME MAP",
        "D. PART 2 — ARGENTINA UNIVERSE SCAN + TOP 5",
        "E. PART 3 — CRYPTO MACRO REGIME MAP",
        "F. PART 4 — CRYPTO TOP 5 THESIS CARDS",
        "G. CROSS-ASSET GLOBAL DRIVER MAP",
        "H. MONITORING DASHBOARD — LIVING SIGNAL TABLE",
        "I. RED TEAM — STEELMAN COUNTERARGUMENTS",
        "J. APPENDIX",
    }

    executive_summary_en = _collect_section_block(
        doc_paragraphs,
        "A. EXECUTIVE SUMMARY",
        stop_titles=stop_titles,
        max_items=6,
    )

    methodology_en = _collect_section_block(
        doc_paragraphs,
        "B. METHODOLOGY + DATA SOURCES",
        stop_titles=stop_titles,
        max_items=8,
    )

    argentina_map_en = _collect_section_block(
        doc_paragraphs,
        "C. PART 1 — ARGENTINA MACRO REGIME MAP",
        stop_titles=stop_titles,
        max_items=9,
    )

    crypto_map_en = _collect_section_block(
        doc_paragraphs,
        "E. PART 3 — CRYPTO MACRO REGIME MAP",
        stop_titles=stop_titles,
        max_items=9,
    )

    risks_en = _collect_section_block(
        doc_paragraphs,
        "I. RED TEAM — STEELMAN COUNTERARGUMENTS",
        stop_titles=stop_titles,
        max_items=7,
    )

    digest = {
        "generated_at_utc": _utc_now_iso(),
        "locale": "es-AR",
        "title": "Research Integrado Argentina + Crypto",
        "documents": [
            {
                "id": "deepresearch_pdf",
                "display_name": "DeepResearch (PDF)",
                "source_file": pdf_path.name,
                "type": "pdf",
                "stats": {
                    "lineas_extraidas": len(pdf_lines),
                },
            },
            {
                "id": "argentina_crypto_docx",
                "display_name": "Argentina+CryptoAnalisis (DOCX)",
                "source_file": docx_path.name,
                "type": "docx",
                "stats": {
                    "parrafos_extraidos": len(doc_paragraphs),
                },
            },
        ],
        "resumen_es": {
            "contexto": [
                "Argentina atraviesa un programa de estabilizacion macro con superavit primario y desaceleracion inflacionaria respecto de 2023.",
                "El acuerdo con FMI y la flexibilizacion parcial de controles cambiarios mejoraron reservas y visibilidad de corto/mediano plazo.",
                "La volatilidad politica y de regimen para 2026 sigue siendo el principal riesgo binario para activos argentinos.",
            ],
            "tesis_argentina_top5": [
                "Vista Energy (VIST)",
                "Grupo Financiero Galicia (GGAL)",
                "BBVA Argentina (BBAR)",
                "Pampa Energia (PAM)",
                "Loma Negra (LOMA)",
            ],
            "tesis_crypto_top5": [
                "Bitcoin (BTC)",
                "Ethereum (ETH)",
                "Solana (SOL)",
                "Hyperliquid (HYPE)",
                "Chainlink (LINK)",
            ],
            "drivers_clave": [
                "Trayectoria de inflacion y sostenibilidad fiscal local.",
                "Pendiente del tipo de cambio y nivel de brecha.",
                "Flujos ETF spot, liquidez global y sesgo de la Fed.",
                "Riesgo electoral argentino de octubre 2026.",
            ],
            "senal_de_alerta": [
                "Aceleracion sostenida de brecha y perdida de reservas.",
                "Suba abrupta de tasas reales en EE.UU. y fortalecimiento del DXY.",
                "Deterioro de flujo institucional a ETF crypto.",
                "Quiebre de disciplina fiscal o retorno de controles extremos.",
            ],
        },
        "notas_metodologicas": {
            "secciones_detectadas": {
                "executive_summary": len(executive_summary_en),
                "methodology": len(methodology_en),
                "argentina_map": len(argentina_map_en),
                "crypto_map": len(crypto_map_en),
                "red_team": len(risks_en),
            },
            "criterio": "Se priorizó síntesis operativa en español preservando estructura del research original.",
        },
    }

    out_path = project_root / "config" / "research_digest.json"
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(digest, handle, ensure_ascii=False, indent=2)

    print(out_path)


if __name__ == "__main__":
    here = Path(__file__).resolve().parent.parent
    build_digest(here)
