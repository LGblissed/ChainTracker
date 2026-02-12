"""Generate realistic sample data for Chain Tracker development.

Run this once to populate data/ with values matching the V4 prototype.
This lets you develop and test the dashboard before the pipeline
has real connectivity.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
TODAY_DIR = DATA_DIR / TODAY
COMMUNITY_DIR = DATA_DIR / "community"

TODAY_DIR.mkdir(parents=True, exist_ok=True)
COMMUNITY_DIR.mkdir(parents=True, exist_ok=True)

PULLED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ── FX Rates (dolarhoy) ──
fx_data = {
    "source_id": "fx_rates_dolarhoy",
    "pulled_at_utc": PULLED_AT,
    "status": "ok",
    "data": {
        "dolar_oficial_compra": 1060.0,
        "dolar_oficial_venta": 1090.0,
        "dolar_blue_compra": 1250.0,
        "dolar_blue_venta": 1280.0,
        "dolar_mep": 1220.0,
        "dolar_ccl": 1235.0,
        "dolar_crypto": 1245.0,
        "brecha_blue_vs_oficial_pct": 17.43
    },
    "errors": [],
    "raw_response_snippet": ""
}

# ── BCRA Reserves ──
reserves_data = {
    "source_id": "bcra_reserves",
    "pulled_at_utc": PULLED_AT,
    "status": "ok",
    "data": {
        "reservas_internacionales_usd_mm": 27500.0,
        "base_monetaria_ars_mm": 12800000.0,
        "data_date": TODAY
    },
    "errors": [],
    "raw_response_snippet": ""
}

# ── US Yields (FRED) ──
yields_data = {
    "source_id": "fred_us_yields",
    "pulled_at_utc": PULLED_AT,
    "status": "ok",
    "data": {
        "us_2y_yield": 4.25,
        "us_10y_yield": 4.52,
        "us_30y_yield": 4.71,
        "data_date": TODAY
    },
    "errors": [],
    "raw_response_snippet": ""
}

# ── Chain Analysis ──
chain_analysis = {
    "date": TODAY,
    "generated_at_utc": PULLED_AT,
    "chain_state": [
        {
            "layer": 1,
            "layer_name": "Global",
            "status": "neutral",
            "label": "neutral",
            "description": "DXY estable. VIX contenido en 14. Commodities laterales, soja sin presión."
        },
        {
            "layer": 2,
            "layer_name": "Transmisión",
            "status": "elevated",
            "label": "elevada",
            "description": "Reservas −USD 150M. EMBI+ +12 bps. CIARA-CEC liquidación semanal +12%."
        },
        {
            "layer": 3,
            "layer_name": "Monetario",
            "status": "stressed",
            "label": "contracción",
            "description": "BCRA absorbió $80B. Tasa real +8%. Base monetaria estable."
        },
        {
            "layer": 4,
            "layer_name": "Mercados",
            "status": "elevated",
            "label": "cauteloso",
            "description": "MERVAL lateral, volumen mínimo mensual. Brecha ampliando. AL30 sin demanda."
        },
        {
            "layer": 5,
            "layer_name": "Regulatorio",
            "status": "neutral",
            "label": "sin cambios",
            "description": "Res. BCRA 2024/15 (técnica, sin impacto). CEPO intacto."
        }
    ],
    "daily_changes": [
        {"type": "up", "label": "Blue", "detail": "$ 1.250 → 1.280 (+2,4%)"},
        {"type": "dn", "label": "Reservas", "detail": "USD 27.650 → 27.500 M"},
        {"type": "up", "label": "Brecha", "detail": "16,8% → 17,4% (+0,6 pp)"},
        {"type": "eq", "label": "10Y Yield", "detail": "4,53% → 4,52% (−1 bp)"},
        {"type": "rg", "label": "Boletín", "detail": "Res. BCRA 2024/15"}
    ],
    "previous_day": {
        "dolar_blue_venta": 1250.0,
        "dolar_oficial_venta": 1090.0,
        "dolar_mep": 1230.0,
        "dolar_ccl": 1222.0,
        "brecha_pct": 16.8,
        "reservas_usd_mm": 27650.0,
        "us_2y_yield": 4.22,
        "us_10y_yield": 4.53,
        "us_30y_yield": 4.69
    },
    "sparklines": {
        "reserves_30d": [28100,28050,27980,27950,27900,27850,27800,27750,27800,27780,27750,27700,27680,27650,27700,27680,27650,27600,27580,27550,27600,27620,27650,27680,27700,27650,27600,27550,27500,27500],
        "brecha_90d": [15.2,15.5,15.8,16.0,15.9,16.1,16.3,16.5,16.2,16.0,15.8,15.9,16.1,16.4,16.6,16.8,16.5,16.3,16.1,16.2,16.4,16.6,16.8,17.0,16.8,16.9,17.0,17.1,17.2,17.4],
        "yields_10y_30d": [4.45,4.48,4.50,4.49,4.51,4.53,4.52,4.50,4.48,4.47,4.49,4.50,4.51,4.53,4.55,4.54,4.52,4.50,4.48,4.49,4.51,4.52,4.53,4.54,4.53,4.52,4.51,4.50,4.51,4.52]
    }
}

# ── Daily Brief ──
daily_brief = """Jornada marcada por la caída de reservas del BCRA (USD 150M), consistente con intervención cambiaria para contener la brecha. Blue +2,4% con oficial estable. Brecha en 17,4%, tercera semana consecutiva ampliando.

CIARA-CEC reportó liquidación semanal +12% — algo de aire para la próxima semana. Yields americanos sin cambios significativos: 10Y en 4,52%, spread 2s10s en 27 bps.

Sin regulatorio de impacto. Res. BCRA 2024/15 es técnica, sin efecto en mercado.

**Puntos de atención:**
- Reservas: tendencia descendente sostenida. Si continúa 3 días más, posible ajuste en crawling peg.
- Brecha: 17,4% no es crisis pero la tendencia de 3 semanas ampliando es señal clara.
- AL30: volumen mínimo mensual. Baja liquidez = movimientos violentos cuando se destapa.
"""

# ── Community Feed ──
feed_data = [
    {
        "id": 1,
        "user": "Lau",
        "timestamp": f"{TODAY}T17:02:00Z",
        "message": "Reservas cayendo fuerte. Si sigue 3 días más, el BCRA va a tener que ajustar el crawling peg."
    },
    {
        "id": 2,
        "user": "Marcos",
        "timestamp": f"{TODAY}T16:45:00Z",
        "message": "Volumen en AL30: mínimo del mes. Nadie mueve ficha. Ojo que cuando se destapa es violento."
    },
    {
        "id": 3,
        "user": "Caro",
        "timestamp": f"{TODAY}T15:30:00Z",
        "message": "Vitelli: liquidación CIARA-CEC +12%. Aire para reservas, pero no alcanza si siguen interviniendo."
    },
    {
        "id": 4,
        "user": "Lau",
        "timestamp": f"{TODAY}T14:20:00Z",
        "message": "Brecha pasó 17%. No es crisis, pero la tendencia es clara. Tres semanas ampliando."
    },
    {
        "id": 5,
        "user": "Tomás",
        "timestamp": f"{TODAY}T12:15:00Z",
        "message": "Marull publicó el FMyA Weekly. Apunta a que el test de reservas viene en marzo si el agro no acelera."
    }
]


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  wrote {path}")


def main():
    print(f"Generating sample data for {TODAY}...")
    write_json(TODAY_DIR / "fx_rates_dolarhoy.json", fx_data)
    write_json(TODAY_DIR / "bcra_reserves.json", reserves_data)
    write_json(TODAY_DIR / "fred_us_yields.json", yields_data)
    write_json(TODAY_DIR / "chain_analysis.json", chain_analysis)

    brief_path = TODAY_DIR / "daily_brief.md"
    brief_path.write_text(daily_brief, encoding="utf-8")
    print(f"  wrote {brief_path}")

    write_json(COMMUNITY_DIR / "feed.json", feed_data)
    print("Done.")


if __name__ == "__main__":
    main()
