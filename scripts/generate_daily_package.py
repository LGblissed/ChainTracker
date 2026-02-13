"""Generate page-ready daily package from pulled source files.

Creates:
- data/{date}/chain_analysis.json
- data/{date}/daily_brief.md
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO format ending with Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_date_folder(name: str) -> bool:
    """Return True when name looks like YYYY-MM-DD."""
    return len(name) == 10 and name[4] == "-" and name[7] == "-" and name.replace("-", "").isdigit()


def _load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file safely and return object or empty dict."""
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
        return payload if isinstance(payload, dict) else {}
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}


def _write_json(path: Path, payload: Dict[str, Any]):
    """Write JSON with UTF-8 and indentation."""
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def _to_float(value: Any) -> float | None:
    """Convert numeric-like values to float safely."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_ar_number(value: float, decimals: int = 2) -> str:
    """Format number with Argentine separators."""
    template = f"{{:,.{decimals}f}}"
    raw = template.format(abs(value))
    integer, _, decimal = raw.partition(".")
    integer = integer.replace(",", ".")
    if decimals == 0:
        return f"-{integer}" if value < 0 else integer
    suffix = f",{decimal}" if decimal else ""
    return f"-{integer}{suffix}" if value < 0 else f"{integer}{suffix}"


def _read_source_metrics(date_dir: Path) -> Dict[str, Any]:
    """Read the three source payloads for a given date directory."""
    fx_raw = _load_json(date_dir / "fx_rates_dolarhoy.json")
    reserves_raw = _load_json(date_dir / "bcra_reserves.json")
    yields_raw = _load_json(date_dir / "fred_us_yields.json")

    fx = fx_raw.get("data", {}) if isinstance(fx_raw.get("data"), dict) else {}
    reserves = reserves_raw.get("data", {}) if isinstance(reserves_raw.get("data"), dict) else {}
    yields = yields_raw.get("data", {}) if isinstance(yields_raw.get("data"), dict) else {}

    return {
        "dolar_blue_venta": _to_float(fx.get("dolar_blue_venta")),
        "dolar_oficial_venta": _to_float(fx.get("dolar_oficial_venta")),
        "dolar_mep": _to_float(fx.get("dolar_mep")),
        "dolar_ccl": _to_float(fx.get("dolar_ccl")),
        "brecha_pct": _to_float(fx.get("brecha_blue_vs_oficial_pct")),
        "reservas_usd_mm": _to_float(reserves.get("reservas_internacionales_usd_mm")),
        "base_monetaria_ars_mm": _to_float(reserves.get("base_monetaria_ars_mm")),
        "us_2y_yield": _to_float(yields.get("us_2y_yield")),
        "us_10y_yield": _to_float(yields.get("us_10y_yield")),
        "us_30y_yield": _to_float(yields.get("us_30y_yield")),
        "pulled_times": [
            fx_raw.get("pulled_at_utc"),
            reserves_raw.get("pulled_at_utc"),
            yields_raw.get("pulled_at_utc"),
        ],
    }


def _pct_change(current: float | None, previous: float | None) -> float | None:
    """Return percent change if both values are valid."""
    if current is None or previous is None or previous == 0:
        return None
    return ((current / previous) - 1.0) * 100.0


def _delta(current: float | None, previous: float | None) -> float | None:
    """Return absolute delta if both values are valid."""
    if current is None or previous is None:
        return None
    return current - previous


def _build_changes(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, float | None]:
    """Compute day-over-day changes from current and previous snapshots."""
    return {
        "blue_pct": _pct_change(current.get("dolar_blue_venta"), previous.get("dolar_blue_venta")),
        "brecha_pp": _delta(current.get("brecha_pct"), previous.get("brecha_pct")),
        "reserves_mm": _delta(current.get("reservas_usd_mm"), previous.get("reservas_usd_mm")),
        "y10_bps": None
        if _delta(current.get("us_10y_yield"), previous.get("us_10y_yield")) is None
        else _delta(current.get("us_10y_yield"), previous.get("us_10y_yield")) * 100.0,
    }


def _change_type(value: float | None, epsilon: float = 0.0001) -> str:
    """Map numeric change to UI code."""
    if value is None:
        return "eq"
    if value > epsilon:
        return "up"
    if value < -epsilon:
        return "dn"
    return "eq"


def _fmt_sign(value: float, decimals: int = 1) -> str:
    """Format signed values for human text."""
    if value > 0:
        return f"+{_format_ar_number(value, decimals)}"
    return _format_ar_number(value, decimals)


def _build_daily_changes(current: Dict[str, Any], previous: Dict[str, Any], changes: Dict[str, float | None]) -> List[Dict[str, str]]:
    """Build the UI list of day-over-day changes."""
    rows: List[Dict[str, str]] = []

    blue_now = current.get("dolar_blue_venta")
    blue_prev = previous.get("dolar_blue_venta")
    if blue_now is not None and blue_prev is not None:
        blue_change = changes.get("blue_pct")
        detail = (
            f"$ {_format_ar_number(blue_prev, 0)} -> {_format_ar_number(blue_now, 0)} "
            f"({_fmt_sign(blue_change, 1)}%)"
            if blue_change is not None
            else f"$ {_format_ar_number(blue_prev, 0)} -> {_format_ar_number(blue_now, 0)}"
        )
        rows.append({"type": _change_type(blue_change), "label": "Blue", "detail": detail})

    reserves_now = current.get("reservas_usd_mm")
    reserves_prev = previous.get("reservas_usd_mm")
    if reserves_now is not None and reserves_prev is not None:
        reserves_delta = changes.get("reserves_mm")
        detail = (
            f"USD {_format_ar_number(reserves_prev, 0)} -> {_format_ar_number(reserves_now, 0)} M "
            f"({_fmt_sign(reserves_delta, 0)} M)"
            if reserves_delta is not None
            else f"USD {_format_ar_number(reserves_prev, 0)} -> {_format_ar_number(reserves_now, 0)} M"
        )
        rows.append({"type": _change_type(reserves_delta), "label": "Reservas", "detail": detail})

    brecha_now = current.get("brecha_pct")
    brecha_prev = previous.get("brecha_pct")
    if brecha_now is not None and brecha_prev is not None:
        brecha_delta = changes.get("brecha_pp")
        detail = (
            f"{_format_ar_number(brecha_prev, 1)}% -> {_format_ar_number(brecha_now, 1)}% "
            f"({_fmt_sign(brecha_delta, 1)} pp)"
            if brecha_delta is not None
            else f"{_format_ar_number(brecha_prev, 1)}% -> {_format_ar_number(brecha_now, 1)}%"
        )
        rows.append({"type": _change_type(brecha_delta), "label": "Brecha", "detail": detail})

    y10_now = current.get("us_10y_yield")
    y10_prev = previous.get("us_10y_yield")
    if y10_now is not None and y10_prev is not None:
        y10_delta = changes.get("y10_bps")
        detail = (
            f"{_format_ar_number(y10_prev, 2)}% -> {_format_ar_number(y10_now, 2)}% "
            f"({_fmt_sign(y10_delta, 0)} bp)"
            if y10_delta is not None
            else f"{_format_ar_number(y10_prev, 2)}% -> {_format_ar_number(y10_now, 2)}%"
        )
        rows.append({"type": _change_type(y10_delta), "label": "10Y Yield", "detail": detail})

    if not rows:
        rows.append({"type": "eq", "label": "Estado", "detail": "Sin historial suficiente para cambios diarios."})

    return rows


def _layer_state_global(changes: Dict[str, float | None], current: Dict[str, Any]) -> Dict[str, Any]:
    """Infer layer 1 state from global yield movement."""
    y10 = current.get("us_10y_yield")
    y10_bps = changes.get("y10_bps")
    if y10 is None:
        return {
            "layer": 1,
            "layer_name": "Global",
            "status": "neutral",
            "label": "sin datos",
            "description": "No hay lectura de yields globales en el corte actual.",
        }
    if y10_bps is None:
        return {
            "layer": 1,
            "layer_name": "Global",
            "status": "neutral",
            "label": "estable",
            "description": f"US 10Y en {_format_ar_number(y10, 2)}%, sin comparacion diaria.",
        }

    abs_move = abs(y10_bps)
    if abs_move >= 10:
        status = "stressed"
        label = "movimiento fuerte"
    elif abs_move >= 4:
        status = "elevated"
        label = "movimiento moderado"
    else:
        status = "neutral"
        label = "estable"

    direction = "subio" if y10_bps > 0 else "bajo" if y10_bps < 0 else "sin cambio"
    return {
        "layer": 1,
        "layer_name": "Global",
        "status": status,
        "label": label,
        "description": f"US 10Y {direction} {_format_ar_number(abs(y10_bps), 0)} bp y cierra en {_format_ar_number(y10, 2)}%.",
    }


def _layer_state_transmission(changes: Dict[str, float | None], current: Dict[str, Any]) -> Dict[str, Any]:
    """Infer layer 2 state from reserve changes."""
    reserves = current.get("reservas_usd_mm")
    reserves_delta = changes.get("reserves_mm")
    if reserves is None:
        return {
            "layer": 2,
            "layer_name": "Transmision",
            "status": "neutral",
            "label": "sin datos",
            "description": "No hay dato de reservas para evaluar transmision.",
        }
    if reserves_delta is None:
        return {
            "layer": 2,
            "layer_name": "Transmision",
            "status": "neutral",
            "label": "estable",
            "description": f"Reservas en USD {_format_ar_number(reserves, 0)} M, sin comparacion diaria.",
        }

    if reserves_delta <= -200:
        status = "stressed"
        label = "presion alta"
    elif reserves_delta <= -50:
        status = "elevated"
        label = "presion moderada"
    else:
        status = "neutral"
        label = "estable"

    direction = "cayeron" if reserves_delta < 0 else "subieron" if reserves_delta > 0 else "sin cambio"
    return {
        "layer": 2,
        "layer_name": "Transmision",
        "status": status,
        "label": label,
        "description": (
            f"Reservas {direction} {_format_ar_number(abs(reserves_delta), 0)} M "
            f"hasta USD {_format_ar_number(reserves, 0)} M."
        ),
    }


def _layer_state_monetary(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    """Infer layer 3 state from base monetaria variation."""
    base_now = current.get("base_monetaria_ars_mm")
    base_prev = previous.get("base_monetaria_ars_mm")
    if base_now is None:
        return {
            "layer": 3,
            "layer_name": "Monetario",
            "status": "neutral",
            "label": "sin datos",
            "description": "No hay dato de base monetaria para este corte.",
        }
    if base_prev is None:
        return {
            "layer": 3,
            "layer_name": "Monetario",
            "status": "neutral",
            "label": "estable",
            "description": f"Base monetaria en ARS {_format_ar_number(base_now, 0)} M, sin comparacion diaria.",
        }

    delta = base_now - base_prev
    if abs(delta) >= 300000:
        status = "elevated"
        label = "movimiento moderado"
    else:
        status = "neutral"
        label = "estable"

    direction = "subio" if delta > 0 else "bajo" if delta < 0 else "sin cambio"
    return {
        "layer": 3,
        "layer_name": "Monetario",
        "status": status,
        "label": label,
        "description": (
            f"Base monetaria {direction} {_format_ar_number(abs(delta), 0)} M "
            f"y queda en ARS {_format_ar_number(base_now, 0)} M."
        ),
    }


def _layer_state_markets(changes: Dict[str, float | None], current: Dict[str, Any]) -> Dict[str, Any]:
    """Infer layer 4 state from FX spread and blue move."""
    brecha = current.get("brecha_pct")
    brecha_delta = changes.get("brecha_pp")
    blue_delta = changes.get("blue_pct")
    if brecha is None:
        return {
            "layer": 4,
            "layer_name": "Mercados",
            "status": "neutral",
            "label": "sin datos",
            "description": "No hay brecha disponible para lectura de mercado local.",
        }

    if brecha >= 25 or (brecha_delta is not None and brecha_delta >= 2):
        status = "stressed"
        label = "tension alta"
    elif brecha >= 15 or (brecha_delta is not None and brecha_delta >= 0.5):
        status = "elevated"
        label = "cauteloso"
    else:
        status = "neutral"
        label = "estable"

    detail = f"Brecha en {_format_ar_number(brecha, 1)}%"
    if brecha_delta is not None:
        detail += f" ({_fmt_sign(brecha_delta, 1)} pp)"
    if blue_delta is not None:
        detail += f", Blue {_fmt_sign(blue_delta, 1)}% d/d"

    return {
        "layer": 4,
        "layer_name": "Mercados",
        "status": status,
        "label": label,
        "description": detail + ".",
    }


def _layer_state_regulatory() -> Dict[str, Any]:
    """Return layer 5 placeholder until regulatory pullers are active."""
    return {
        "layer": 5,
        "layer_name": "Regulatorio",
        "status": "neutral",
        "label": "manual",
        "description": "Sin ingesta automatica regulatoria todavia. Revisar Boletin Oficial y BCRA manualmente.",
    }


def _collect_sparkline(data_dir: Path, source_file: str, field_name: str, limit: int) -> List[float]:
    """Collect a numeric timeseries from dated folders."""
    points: List[float] = []
    date_dirs = sorted([item for item in data_dir.iterdir() if item.is_dir() and _is_date_folder(item.name)], key=lambda item: item.name)
    for date_dir in date_dirs:
        payload = _load_json(date_dir / source_file)
        data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        value = _to_float(data.get(field_name))
        if value is not None:
            points.append(value)
    if len(points) > limit:
        return points[-limit:]
    return points


def _build_daily_brief(current: Dict[str, Any], changes: Dict[str, float | None], chain_state: List[Dict[str, Any]]) -> str:
    """Build markdown brief from available pulled metrics only."""
    lines: List[str] = []
    lines.append("Resumen automatico generado desde fuentes activas (FRED, BCRA, DolarHoy).")
    lines.append("")

    blue = current.get("dolar_blue_venta")
    oficial = current.get("dolar_oficial_venta")
    brecha = current.get("brecha_pct")
    reserves = current.get("reservas_usd_mm")
    y10 = current.get("us_10y_yield")

    primary = []
    if blue is not None and oficial is not None:
        primary.append(f"Blue {_format_ar_number(blue, 0)} vs Oficial {_format_ar_number(oficial, 0)}")
    if brecha is not None:
        delta_txt = ""
        if changes.get("brecha_pp") is not None:
            delta_txt = f" ({_fmt_sign(changes['brecha_pp'], 1)} pp d/d)"
        primary.append(f"Brecha {_format_ar_number(brecha, 1)}%{delta_txt}")
    if reserves is not None:
        delta_txt = ""
        if changes.get("reserves_mm") is not None:
            delta_txt = f" ({_fmt_sign(changes['reserves_mm'], 0)} M d/d)"
        primary.append(f"Reservas USD {_format_ar_number(reserves, 0)} M{delta_txt}")
    if y10 is not None:
        delta_txt = ""
        if changes.get("y10_bps") is not None:
            delta_txt = f" ({_fmt_sign(changes['y10_bps'], 0)} bp d/d)"
        primary.append(f"US 10Y {_format_ar_number(y10, 2)}%{delta_txt}")

    if primary:
        lines.append(" | ".join(primary) + ".")
    else:
        lines.append("No hay datos suficientes para resumen numerico hoy.")

    lines.append("")
    lines.append("**Puntos de atencion:**")
    meaningful = [layer for layer in chain_state if layer.get("status") in {"elevated", "stressed"}]
    if meaningful:
        for layer in meaningful:
            lines.append(f"- {layer.get('layer_name')}: {layer.get('description')}")
    else:
        lines.append("- Sin alertas fuertes por reglas automaticas en este corte.")
    lines.append("- Capa regulatoria se mantiene en monitoreo manual hasta activar pullers de normas.")

    return "\n".join(lines).strip() + "\n"


def generate_daily_package(project_root: Path, date_str: str | None = None) -> Dict[str, Any]:
    """Generate daily chain analysis and brief from source pull outputs."""
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    data_dir = project_root / "data"
    date_dir = data_dir / date_str
    date_dir.mkdir(parents=True, exist_ok=True)

    date_folders = sorted([item.name for item in data_dir.iterdir() if item.is_dir() and _is_date_folder(item.name)])
    previous_date = None
    for item in date_folders:
        if item < date_str:
            previous_date = item

    current = _read_source_metrics(date_dir)
    previous = _read_source_metrics(data_dir / previous_date) if previous_date else {}

    # Ensure the expected keys exist for app calculations.
    previous_day = {
        "dolar_blue_venta": previous.get("dolar_blue_venta"),
        "dolar_oficial_venta": previous.get("dolar_oficial_venta"),
        "dolar_mep": previous.get("dolar_mep"),
        "dolar_ccl": previous.get("dolar_ccl"),
        "brecha_pct": previous.get("brecha_pct"),
        "reservas_usd_mm": previous.get("reservas_usd_mm"),
        "base_monetaria_ars_mm": previous.get("base_monetaria_ars_mm"),
        "us_2y_yield": previous.get("us_2y_yield"),
        "us_10y_yield": previous.get("us_10y_yield"),
        "us_30y_yield": previous.get("us_30y_yield"),
    }

    changes = _build_changes(current=current, previous=previous_day)

    chain_state = [
        _layer_state_global(changes=changes, current=current),
        _layer_state_transmission(changes=changes, current=current),
        _layer_state_monetary(current=current, previous=previous_day),
        _layer_state_markets(changes=changes, current=current),
        _layer_state_regulatory(),
    ]
    daily_changes = _build_daily_changes(current=current, previous=previous_day, changes=changes)

    chain_analysis = {
        "date": date_str,
        "generated_at_utc": _utc_now_iso(),
        "chain_state": chain_state,
        "daily_changes": daily_changes,
        "previous_day": previous_day,
        "sparklines": {
            "reserves_30d": _collect_sparkline(
                data_dir=data_dir,
                source_file="bcra_reserves.json",
                field_name="reservas_internacionales_usd_mm",
                limit=30,
            ),
            "brecha_90d": _collect_sparkline(
                data_dir=data_dir,
                source_file="fx_rates_dolarhoy.json",
                field_name="brecha_blue_vs_oficial_pct",
                limit=90,
            ),
            "yields_10y_30d": _collect_sparkline(
                data_dir=data_dir,
                source_file="fred_us_yields.json",
                field_name="us_10y_yield",
                limit=30,
            ),
        },
    }

    brief_md = _build_daily_brief(current=current, changes=changes, chain_state=chain_state)

    _write_json(date_dir / "chain_analysis.json", chain_analysis)
    with open(date_dir / "daily_brief.md", "w", encoding="utf-8") as file_obj:
        file_obj.write(brief_md)

    warnings: List[str] = []
    if previous_date is None:
        warnings.append("No previous date snapshot found; day-over-day changes may be incomplete.")
    if current.get("dolar_blue_venta") is None and current.get("reservas_usd_mm") is None and current.get("us_10y_yield") is None:
        warnings.append("No core source metrics were found in today's files.")

    return {
        "status": "ok",
        "date": date_str,
        "generated_files": ["chain_analysis.json", "daily_brief.md"],
        "warnings": warnings,
    }

