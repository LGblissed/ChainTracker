"""Chain Tracker — Argentina Macro Intelligence Dashboard.

Flask web server. Reads pipeline JSON data and serves the
Café con Leche dashboard via Jinja2 templates.
"""

import html
import json
import os
import secrets
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify
)
import markdown
from dotenv import load_dotenv

# ── Paths ──
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"
load_dotenv(PROJECT_ROOT / ".env")

# ── Load app config ──
with open(CONFIG_DIR / "app_config.json", encoding="utf-8") as f:
    APP_CONFIG = json.load(f)

# ── Flask app ──
app = Flask(
    __name__,
    template_folder=str(PROJECT_ROOT / "templates"),
    static_folder=str(PROJECT_ROOT / "static"),
)
APP_PASSWORD = (os.getenv("CHAIN_TRACKER_PASSWORD") or "").strip()
APP_COOKIE_SECRET = (os.getenv("CHAIN_TRACKER_COOKIE_SECRET") or "").strip()
SESSION_SECRET_CONFIGURED = bool(APP_COOKIE_SECRET)
app.secret_key = APP_COOKIE_SECRET or secrets.token_hex(32)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
if os.getenv("CHAIN_TRACKER_SECURE_COOKIES", "0") == "1":
    app.config["SESSION_COOKIE_SECURE"] = True


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING
# ═══════════════════════════════════════════════════════════════

def get_latest_date():
    """Find the most recent date directory in data/."""
    if not DATA_DIR.exists():
        return None
    date_dirs = sorted(
        [d.name for d in DATA_DIR.iterdir()
         if d.is_dir() and d.name[:4].isdigit()],
        reverse=True,
    )
    return date_dirs[0] if date_dirs else None


def load_json(path):
    """Load a JSON file. Returns empty dict on any error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def load_markdown_file(path):
    """Load a markdown file and convert to HTML."""
    try:
        text = Path(path).read_text(encoding="utf-8")
        # Escape inline HTML before markdown rendering.
        safe_text = html.escape(text)
        return markdown.markdown(safe_text)
    except (FileNotFoundError, OSError):
        return ""


def compute_pipeline_status():
    """Compute pipeline status from pull log."""
    log_path = LOGS_DIR / "pull_log.jsonl"
    if not log_path.exists():
        # Fallback for environments where logs are not persisted.
        last_run = "—"
        latest_date = get_latest_date()
        if latest_date:
            date_dir = DATA_DIR / latest_date
            pulled_times = []
            for source_id in ["fx_rates_dolarhoy", "bcra_reserves", "fred_us_yields"]:
                payload = load_json(date_dir / f"{source_id}.json")
                pulled_at = payload.get("pulled_at_utc")
                if isinstance(pulled_at, str) and pulled_at:
                    pulled_times.append(pulled_at)
            if pulled_times:
                try:
                    dt = max(datetime.fromisoformat(item.replace("Z", "+00:00")) for item in pulled_times)
                    last_run = dt.strftime("%H:%M")
                except ValueError:
                    last_run = "—"

        registry = load_json(CONFIG_DIR / "source_registry.json")
        total = len(registry) if isinstance(registry, list) else 33
        active = sum(1 for s in registry if isinstance(s, dict) and s.get("active"))
        return {"active": active, "total": total, "last_run": last_run}

    last_run = "—"
    sources_seen = set()
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                sources_seen.add(entry.get("source_id", ""))
                last_run = entry.get("pulled_at_utc", last_run)
    except (json.JSONDecodeError, OSError):
        pass

    # Count active sources from registry
    registry = load_json(CONFIG_DIR / "source_registry.json")
    total = len(registry) if isinstance(registry, list) else 33
    active = sum(1 for s in registry if isinstance(s, dict) and s.get("active"))

    # Format last run time
    if last_run != "—":
        try:
            dt = datetime.fromisoformat(last_run.replace("Z", "+00:00"))
            last_run = dt.strftime("%H:%M")
        except ValueError:
            pass

    return {"active": active, "total": total, "last_run": last_run}


def compute_source_health():
    """Build active-source health status, including missing files."""
    latest_date = get_latest_date()
    date_dir = DATA_DIR / latest_date if latest_date else None
    registry = load_json(CONFIG_DIR / "source_registry.json")
    if not isinstance(registry, list):
        registry = []

    file_aliases = {
        "dolarhoy_fx": "fx_rates_dolarhoy.json",
        "fx_rates_dolarhoy": "fx_rates_dolarhoy.json",
    }
    status_rows = []
    summary = {"active_total": 0, "ok": 0, "missing": 0, "error": 0}

    for source in registry:
        if not isinstance(source, dict):
            continue
        source_id = source.get("source_id", "")
        active = bool(source.get("active"))
        if active:
            summary["active_total"] += 1
        payload = {}
        filename = file_aliases.get(source_id, f"{source_id}.json")
        if date_dir and (date_dir / filename).exists():
            payload = load_json(date_dir / filename)

        status = "missing"
        if payload:
            status = payload.get("status", "error") or "error"
            if status not in {"ok", "error"}:
                status = "error"

        if active:
            if status == "ok":
                summary["ok"] += 1
            elif status == "missing":
                summary["missing"] += 1
            else:
                summary["error"] += 1

        status_rows.append(
            {
                "source_id": source_id,
                "name": source.get("name", source_id),
                "layer": source.get("layer"),
                "tier": source.get("credibility_tier", "—"),
                "active": active,
                "status": status,
                "last_verified": source.get("last_verified", ""),
                "pulled_at_utc": payload.get("pulled_at_utc", ""),
                "url": source.get("url", ""),
                "known_bias": source.get("known_bias"),
                "data_points": source.get("data_points", []),
            }
        )

    status_rows.sort(key=lambda row: (not row["active"], row.get("layer") or 99, row["name"].lower()))
    return {"summary": summary, "rows": status_rows, "latest_date": latest_date}


def load_research_digest():
    """Load generated research digest if available."""
    payload = load_json(CONFIG_DIR / "research_digest.json")
    if not isinstance(payload, dict):
        payload = {}
    return payload


def load_analyst_registry():
    """Load analyst registry for analyst page."""
    payload = load_json(CONFIG_DIR / "analyst_registry.json")
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def get_history_rows(limit=120):
    """Build historical rows from available date snapshots."""
    rows = []
    if not DATA_DIR.exists():
        return rows

    date_dirs = sorted(
        [item for item in DATA_DIR.iterdir() if item.is_dir() and item.name[:4].isdigit()],
        key=lambda item: item.name,
        reverse=True,
    )

    for date_dir in date_dirs[:limit]:
        fx_raw = load_json(date_dir / "fx_rates_dolarhoy.json")
        res_raw = load_json(date_dir / "bcra_reserves.json")
        yld_raw = load_json(date_dir / "fred_us_yields.json")

        fx = fx_raw.get("data", {}) if isinstance(fx_raw.get("data"), dict) else {}
        reserves = res_raw.get("data", {}) if isinstance(res_raw.get("data"), dict) else {}
        yields = yld_raw.get("data", {}) if isinstance(yld_raw.get("data"), dict) else {}

        rows.append(
            {
                "date": date_dir.name,
                "blue": fx.get("dolar_blue_venta"),
                "oficial": fx.get("dolar_oficial_venta"),
                "brecha": fx.get("brecha_blue_vs_oficial_pct"),
                "reservas": reserves.get("reservas_internacionales_usd_mm"),
                "us10y": yields.get("us_10y_yield"),
                "fx_status": fx_raw.get("status", "missing"),
                "res_status": res_raw.get("status", "missing"),
                "yld_status": yld_raw.get("status", "missing"),
            }
        )

    return rows


def get_layer_rollup():
    """Summarize source coverage by layer for quick diagnostics."""
    source_health = compute_source_health()
    grouped = defaultdict(lambda: {"total": 0, "active": 0, "ok": 0, "missing_or_error": 0})
    for row in source_health["rows"]:
        layer = row.get("layer") or 0
        grouped[layer]["total"] += 1
        if row.get("active"):
            grouped[layer]["active"] += 1
            if row.get("status") == "ok":
                grouped[layer]["ok"] += 1
            else:
                grouped[layer]["missing_or_error"] += 1

    result = []
    for layer in sorted(grouped):
        item = grouped[layer]
        item["layer"] = layer
        result.append(item)
    return result


def get_overview_data():
    """Load all data needed for the Overview page."""
    date = get_latest_date()
    if not date:
        return {
            "has_data": False,
            "date": None,
            "pipeline": compute_pipeline_status(),
            "updated": "",
            "updated_rel": "",
        }

    date_dir = DATA_DIR / date

    # Load raw JSON files
    fx_raw = load_json(date_dir / "fx_rates_dolarhoy.json")
    res_raw = load_json(date_dir / "bcra_reserves.json")
    yld_raw = load_json(date_dir / "fred_us_yields.json")
    chain_raw = load_json(date_dir / "chain_analysis.json")
    brief_html = load_markdown_file(date_dir / "daily_brief.md")

    # Extract data payloads
    fx = fx_raw.get("data", {})
    res = res_raw.get("data", {})
    yld = yld_raw.get("data", {})

    # Pipeline status
    pipeline = compute_pipeline_status()

    # Compute changes from chain_analysis previous_day
    prev = chain_raw.get("previous_day", {})

    # FX changes
    blue_prev = prev.get("dolar_blue_venta")
    blue_now = fx.get("dolar_blue_venta")
    blue_change = None
    if blue_prev and blue_now and blue_prev != 0:
        blue_change = round(((blue_now / blue_prev) - 1) * 100, 1)

    mep_prev = prev.get("dolar_mep")
    mep_now = fx.get("dolar_mep")
    mep_change = None
    if mep_prev and mep_now and mep_prev != 0:
        mep_change = round(((mep_now / mep_prev) - 1) * 100, 1)

    ccl_prev = prev.get("dolar_ccl")
    ccl_now = fx.get("dolar_ccl")
    ccl_change = None
    if ccl_prev and ccl_now and ccl_prev != 0:
        ccl_change = round(((ccl_now / ccl_prev) - 1) * 100, 1)

    brecha_prev = prev.get("brecha_pct")
    brecha_now = fx.get("brecha_blue_vs_oficial_pct")
    brecha_change = None
    if brecha_prev is not None and brecha_now is not None:
        brecha_change = round(brecha_now - brecha_prev, 1)

    # Reserve changes
    res_prev = prev.get("reservas_usd_mm")
    res_now = res.get("reservas_internacionales_usd_mm")
    res_change = None
    if res_prev is not None and res_now is not None:
        res_change = round(res_now - res_prev, 0)

    # Yield changes (basis points)
    y2_prev = prev.get("us_2y_yield")
    y2_now = yld.get("us_2y_yield")
    y2_change = None
    if y2_prev is not None and y2_now is not None:
        y2_change = round((y2_now - y2_prev) * 100, 0)

    y10_prev = prev.get("us_10y_yield")
    y10_now = yld.get("us_10y_yield")
    y10_change = None
    if y10_prev is not None and y10_now is not None:
        y10_change = round((y10_now - y10_prev) * 100, 0)

    y30_prev = prev.get("us_30y_yield")
    y30_now = yld.get("us_30y_yield")
    y30_change = None
    if y30_prev is not None and y30_now is not None:
        y30_change = round((y30_now - y30_prev) * 100, 0)

    # 2s10s spread
    spread = None
    spread_change = None
    if y10_now is not None and y2_now is not None:
        spread = round((y10_now - y2_now) * 100, 0)
    if y10_prev is not None and y2_prev is not None and spread is not None:
        prev_spread = round((y10_prev - y2_prev) * 100, 0)
        spread_change = spread - prev_spread

    # Sparkline data
    sparklines = chain_raw.get("sparklines", {})

    # Source freshness
    fx_status = fx_raw.get("status", "error")
    res_status = res_raw.get("status", "error")
    yld_status = yld_raw.get("status", "error")

    # Updated timestamp
    pulled_times = [
        fx_raw.get("pulled_at_utc", ""),
        res_raw.get("pulled_at_utc", ""),
        yld_raw.get("pulled_at_utc", ""),
    ]
    latest_pull = max((t for t in pulled_times if t), default="")
    updated_display = ""
    updated_rel = ""
    if latest_pull:
        try:
            dt = datetime.fromisoformat(latest_pull.replace("Z", "+00:00"))
            updated_display = dt.strftime("%d %b %Y · %H:%M ART")
            delta = datetime.now(timezone.utc) - dt
            mins = int(delta.total_seconds() / 60)
            if mins < 60:
                updated_rel = f"hace {mins}m"
            elif mins < 1440:
                updated_rel = f"hace {mins // 60}h"
            else:
                updated_rel = f"hace {mins // 1440}d"
        except ValueError:
            updated_display = latest_pull
            updated_rel = ""

    return {
        "has_data": True,
        "date": date,
        "fx": {
            "oficial": fx.get("dolar_oficial_venta"),
            "blue": fx.get("dolar_blue_venta"),
            "blue_change": blue_change,
            "mep": fx.get("dolar_mep"),
            "mep_change": mep_change,
            "ccl": fx.get("dolar_ccl"),
            "ccl_change": ccl_change,
            "brecha": brecha_now,
            "brecha_change": brecha_change,
            "status": fx_status,
        },
        "reserves": {
            "value": res_now,
            "change": res_change,
            "sparkline": sparklines.get("reserves_30d", []),
            "status": res_status,
        },
        "yields": {
            "y2": y2_now,
            "y2_change": y2_change,
            "y10": y10_now,
            "y10_change": y10_change,
            "y30": y30_now,
            "y30_change": y30_change,
            "spread": spread,
            "spread_change": spread_change,
            "sparkline": sparklines.get("yields_10y_30d", []),
            "status": yld_status,
        },
        "chain": chain_raw.get("chain_state", []),
        "changes": chain_raw.get("daily_changes", []),
        "brief": brief_html,
        "brecha_sparkline": sparklines.get("brecha_90d", []),
        "pipeline": pipeline,
        "updated": updated_display,
        "updated_rel": updated_rel,
    }


def get_base_page_context():
    """Common context values shared by most pages."""
    overview = get_overview_data()
    return {
        "pipeline": overview.get("pipeline", compute_pipeline_status()),
        "updated": overview.get("updated", ""),
        "updated_rel": overview.get("updated_rel", ""),
        "date": overview.get("date"),
    }


# ═══════════════════════════════════════════════════════════════
#  AUTH HELPERS
# ═══════════════════════════════════════════════════════════════

def is_authenticated():
    return session.get("authenticated") is True and session.get("display_name")


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.before_request
def require_auth():
    """Redirect to login for all pages except /login and /static."""
    allowed = ("login", "api_auth", "static")
    if request.endpoint and request.endpoint not in allowed:
        if not is_authenticated():
            return redirect(url_for("login"))


@app.route("/login", methods=["GET"])
def login():
    if is_authenticated():
        return redirect(url_for("overview"))
    return render_template("login.html")


@app.route("/api/auth", methods=["POST"])
def api_auth():
    """Handle login: password check and name setting."""
    data = request.get_json(silent=True) or {}
    action = data.get("action", "")

    if action == "password":
        pw = (data.get("password") or "").strip()
        if not APP_PASSWORD:
            return jsonify({"ok": False, "error": "Password not configured on server"}), 500
        if not SESSION_SECRET_CONFIGURED:
            return jsonify({"ok": False, "error": "Session secret not configured on server"}), 500
        if pw == APP_PASSWORD:
            session["authenticated"] = True
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Contraseña incorrecta"})

    elif action == "name":
        name = (data.get("name") or "").strip()
        if not SESSION_SECRET_CONFIGURED:
            return jsonify({"ok": False, "error": "Session secret not configured on server"}), 500
        if not session.get("authenticated"):
            return jsonify({"ok": False, "error": "Sesión expirada. Reintenta la contraseña."}), 401
        if len(name) >= 2 and session.get("authenticated"):
            session["display_name"] = name
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Nombre inválido"})

    return jsonify({"ok": False, "error": "Acción no reconocida"})


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Pages ──

@app.route("/")
def overview():
    data = get_overview_data()
    return render_template(
        "overview.html",
        page_id="overview",
        page_title="Panel",
        user=session.get("display_name", ""),
        source_health=compute_source_health(),
        **data,
    )


@app.route("/chain")
def chain():
    overview = get_overview_data()
    return render_template(
        "chain.html",
        page_id="chain",
        page_title="Cadena",
        user=session.get("display_name", ""),
        chain=overview.get("chain", []),
        changes=overview.get("changes", []),
        source_rollup=get_layer_rollup(),
        **get_base_page_context(),
    )


@app.route("/sources")
def sources():
    source_health = compute_source_health()
    return render_template(
        "sources.html",
        page_id="sources",
        page_title="Fuentes",
        user=session.get("display_name", ""),
        source_health=source_health,
        **get_base_page_context(),
    )


@app.route("/analysts")
def analysts():
    analysts_data = load_analyst_registry()
    return render_template(
        "analysts.html",
        page_id="analysts",
        page_title="Analistas",
        user=session.get("display_name", ""),
        analysts=analysts_data,
        **get_base_page_context(),
    )


@app.route("/history")
def history():
    rows = get_history_rows()
    return render_template(
        "history.html",
        page_id="history",
        page_title="Historia",
        user=session.get("display_name", ""),
        history_rows=rows,
        **get_base_page_context(),
    )


@app.route("/brief")
def brief():
    date = get_latest_date()
    brief_html = ""
    if date:
        brief_html = load_markdown_file(DATA_DIR / date / "daily_brief.md")
    return render_template(
        "brief.html",
        page_id="brief",
        page_title="Resumen",
        user=session.get("display_name", ""),
        brief=brief_html,
        source_rollup=get_layer_rollup(),
        **get_base_page_context(),
    )


@app.route("/research")
def research():
    digest = load_research_digest()
    return render_template(
        "research.html",
        page_id="research",
        page_title="Investigación",
        user=session.get("display_name", ""),
        research=digest,
        source_rollup=get_layer_rollup(),
        **get_base_page_context(),
    )


@app.route("/feed")
def feed():
    feed_data = load_json(DATA_DIR / "community" / "feed.json")
    if not isinstance(feed_data, list):
        feed_data = []
    return render_template(
        "feed.html",
        page_id="feed",
        page_title="Comunidad",
        user=session.get("display_name", ""),
        **get_base_page_context(),
        feed=feed_data,
    )


# ── Feed API ──

@app.route("/api/feed", methods=["GET"])
def api_feed_get():
    feed_data = load_json(DATA_DIR / "community" / "feed.json")
    if not isinstance(feed_data, list):
        feed_data = []
    return jsonify(feed_data)


@app.route("/api/feed", methods=["POST"])
def api_feed_post():
    if not is_authenticated():
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()

    if len(message) < 2 or len(message) > 280:
        return jsonify({"ok": False, "error": "Mensaje debe tener 2-280 caracteres"}), 400

    feed_path = DATA_DIR / "community" / "feed.json"
    feed_data = load_json(feed_path)
    if not isinstance(feed_data, list):
        feed_data = []

    # Generate next ID
    max_id = max((item.get("id", 0) for item in feed_data), default=0)

    new_post = {
        "id": max_id + 1,
        "user": session.get("display_name", "Anon"),
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "message": message,
    }

    feed_data.insert(0, new_post)

    try:
        with open(feed_path, "w", encoding="utf-8") as f:
            json.dump(feed_data, f, ensure_ascii=False, indent=2)
    except OSError:
        return jsonify({"ok": False, "error": "Error guardando"}), 500

    return jsonify({"ok": True, "post": new_post})


# ═══════════════════════════════════════════════════════════════
#  TEMPLATE FILTERS
# ═══════════════════════════════════════════════════════════════

@app.template_filter("fmt_ar")
def format_argentine(value, decimals=2):
    """Format number in Argentine convention: dot thousands, comma decimals."""
    if value is None:
        return "—"
    try:
        value = float(value)
    except (ValueError, TypeError):
        return "—"

    if decimals == 0:
        integer_part = f"{abs(int(value)):,}".replace(",", ".")
        return f"-{integer_part}" if value < 0 else integer_part

    formatted = f"{abs(value):,.{decimals}f}"
    # Swap separators: 1,234.56 → 1.234,56
    parts = formatted.split(".")
    integer_part = parts[0].replace(",", ".")
    decimal_part = parts[1] if len(parts) > 1 else ""
    result = f"{integer_part},{decimal_part}" if decimal_part else integer_part
    return f"-{result}" if value < 0 else result


@app.template_filter("fmt_pct")
def format_percent(value, decimals=1):
    """Format percentage in Argentine convention."""
    if value is None:
        return "—"
    return format_argentine(value, decimals) + "%"


@app.template_filter("to_json")
def to_json_filter(value):
    """Serialize to JSON for embedding in JS."""
    return json.dumps(value, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
#  RUN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.getenv("CHAIN_TRACKER_PORT", APP_CONFIG.get("port", 5000)))
    host = os.getenv("CHAIN_TRACKER_HOST", "127.0.0.1")
    debug_mode = os.getenv("CHAIN_TRACKER_DEBUG", "0") == "1"
    print(f"\n  Chain Tracker - http://{host}:{port}")
    print("  Password: configured via CHAIN_TRACKER_PASSWORD (.env)\n")
    app.run(host=host, port=port, debug=debug_mode)
