"""Configuration and data health validator for Argentina Chain Tracker.

This script acts as an immune system gate for project integrity.
Exit code:
- 0 when all validation categories pass
- 1 when any category fails
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Tuple


SOURCE_REQUIRED_FIELDS = [
    "source_id",
    "name",
    "url",
    "layer",
    "data_points",
    "frequency",
    "format",
    "credibility_tier",
    "api_available",
    "scrape_required",
    "known_bias",
    "active",
    "puller_module",
    "last_verified",
]

ANALYST_REQUIRED_FIELDS = [
    "analyst_id",
    "name",
    "specialty",
    "background",
    "methodology_visibility",
    "known_bias",
    "platforms",
    "affiliation",
    "accuracy_log",
]

SOURCE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
ANALYST_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SOURCE_FREQ_ALLOWED = {"realtime", "daily", "weekly", "monthly", "irregular"}
SOURCE_TIER_ALLOWED = {"T1", "T2", "T3", "T4", "T5"}
ANALYST_VIS_ALLOWED = {"extreme", "high", "medium", "low"}


def _format_errors(errors: List[str], max_items: int = 3) -> str:
    """Format top errors for compact report output."""
    if not errors:
        return ""
    if len(errors) <= max_items:
        return " | ".join(errors)
    shown = " | ".join(errors[:max_items])
    return f"{shown} | ... +{len(errors) - max_items} more"


def _load_json(path: Path, expected_type: type, label: str) -> Tuple[Any, List[str]]:
    """Load JSON file and enforce top-level type."""
    errors: List[str] = []
    if not path.exists():
        return None, [f"{label}: file missing at {path.as_posix()}"]

    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except json.JSONDecodeError as exc:
        return None, [f"{label}: invalid JSON at {path.as_posix()} ({exc})"]
    except OSError as exc:
        return None, [f"{label}: read error at {path.as_posix()} ({exc})"]

    if not isinstance(payload, expected_type):
        return None, [f"{label}: expected {expected_type.__name__} at {path.as_posix()}, got {type(payload).__name__}"]
    return payload, errors


def validate_source_registry(path: Path) -> Tuple[bool, str, Dict[str, Any]]:
    """Validate source registry schema and domain rules."""
    sources, errors = _load_json(path, list, "source_registry")
    if errors:
        return False, _format_errors(errors), {"sources": [], "active_count": 0, "puller_modules": set()}

    assert isinstance(sources, list)
    seen_ids: Dict[str, int] = {}
    seen_urls: Dict[str, str] = {}
    puller_modules: set[str] = set()
    active_count = 0

    for idx, source in enumerate(sources):
        loc = f"{path.as_posix()}[{idx}]"
        if not isinstance(source, dict):
            errors.append(f"{loc}: entry must be object, got {type(source).__name__}")
            continue

        missing = [field for field in SOURCE_REQUIRED_FIELDS if field not in source]
        if missing:
            errors.append(f"{loc}: missing required fields {missing}")
            continue

        source_id = source.get("source_id")
        if not isinstance(source_id, str):
            errors.append(f"{loc}: field source_id must be string, got {type(source_id).__name__}")
        else:
            if not SOURCE_ID_PATTERN.match(source_id):
                errors.append(f"{loc}: source_id '{source_id}' fails snake_case regex ^[a-z][a-z0-9_]*$")
            if source_id in seen_ids:
                errors.append(
                    f"{loc}: duplicate source_id '{source_id}' also present at index {seen_ids[source_id]}"
                )
            else:
                seen_ids[source_id] = idx

        layer = source.get("layer")
        if not isinstance(layer, int) or layer < 1 or layer > 5:
            errors.append(f"{loc}: layer must be integer 1-5, got {layer!r}")

        tier = source.get("credibility_tier")
        if tier not in SOURCE_TIER_ALLOWED:
            errors.append(f"{loc}: credibility_tier must be one of {sorted(SOURCE_TIER_ALLOWED)}, got {tier!r}")

        frequency = source.get("frequency")
        if frequency not in SOURCE_FREQ_ALLOWED:
            errors.append(f"{loc}: frequency must be one of {sorted(SOURCE_FREQ_ALLOWED)}, got {frequency!r}")

        url = source.get("url")
        if not isinstance(url, str) or not url.startswith("https://"):
            errors.append(f"{loc}: url must start with https://, got {url!r}")
        else:
            normalized = url.rstrip("/").lower()
            if normalized in seen_urls:
                errors.append(
                    f"{loc}: duplicate URL '{url}' conflicts with source_id '{seen_urls[normalized]}'"
                )
            else:
                seen_urls[normalized] = str(source.get("source_id", f"idx_{idx}"))

        data_points = source.get("data_points")
        if not isinstance(data_points, list) or len(data_points) == 0:
            errors.append(f"{loc}: data_points must be non-empty array")

        active = source.get("active")
        if not isinstance(active, bool):
            errors.append(f"{loc}: active must be boolean, got {type(active).__name__}")
            active = False

        puller_module = source.get("puller_module")
        if active:
            active_count += 1
            if puller_module is None:
                errors.append(f"{loc}: active=true requires puller_module, got null")
            elif not isinstance(puller_module, str) or not puller_module.strip():
                errors.append(f"{loc}: active=true requires non-empty puller_module string, got {puller_module!r}")
        else:
            if puller_module is not None:
                note = source.get("inactive_reason") or source.get("inactive_note")
                if not note:
                    errors.append(
                        f"{loc}: active=false with puller_module={puller_module!r} requires inactive_note/inactive_reason"
                    )

        if isinstance(puller_module, str) and puller_module.strip():
            puller_modules.add(puller_module.strip())

    if errors:
        return False, _format_errors(errors), {"sources": sources, "active_count": active_count, "puller_modules": puller_modules}

    return True, f"{len(sources)} sources, {active_count} active", {
        "sources": sources,
        "active_count": active_count,
        "puller_modules": puller_modules,
    }


def validate_analyst_registry(path: Path) -> Tuple[bool, str, Dict[str, Any]]:
    """Validate analyst registry schema and domain rules."""
    analysts, errors = _load_json(path, list, "analyst_registry")
    if errors:
        return False, _format_errors(errors), {}

    assert isinstance(analysts, list)
    seen_ids: Dict[str, int] = {}

    for idx, analyst in enumerate(analysts):
        loc = f"{path.as_posix()}[{idx}]"
        if not isinstance(analyst, dict):
            errors.append(f"{loc}: entry must be object, got {type(analyst).__name__}")
            continue

        missing = [field for field in ANALYST_REQUIRED_FIELDS if field not in analyst]
        if missing:
            errors.append(f"{loc}: missing required fields {missing}")
            continue

        analyst_id = analyst.get("analyst_id")
        if not isinstance(analyst_id, str):
            errors.append(f"{loc}: analyst_id must be string, got {type(analyst_id).__name__}")
        else:
            if not ANALYST_ID_PATTERN.match(analyst_id):
                errors.append(f"{loc}: analyst_id '{analyst_id}' fails snake_case regex ^[a-z][a-z0-9_]*$")
            if analyst_id in seen_ids:
                errors.append(f"{loc}: duplicate analyst_id '{analyst_id}', also at index {seen_ids[analyst_id]}")
            else:
                seen_ids[analyst_id] = idx

        visibility = analyst.get("methodology_visibility")
        if visibility not in ANALYST_VIS_ALLOWED:
            errors.append(
                f"{loc}: methodology_visibility must be one of {sorted(ANALYST_VIS_ALLOWED)}, got {visibility!r}"
            )

        specialty = analyst.get("specialty")
        if not isinstance(specialty, list) or len(specialty) == 0:
            errors.append(f"{loc}: specialty must be non-empty array")

        accuracy_log = analyst.get("accuracy_log")
        if not isinstance(accuracy_log, list):
            errors.append(f"{loc}: accuracy_log must be array, got {type(accuracy_log).__name__}")

    if errors:
        return False, _format_errors(errors), {}
    return True, f"{len(analysts)} analysts", {"analysts": analysts}


def validate_competitive_benchmark(path: Path) -> Tuple[bool, str, Dict[str, Any]]:
    """Validate competitive benchmark schema integrity."""
    benchmark, errors = _load_json(path, dict, "competitive_benchmark")
    if errors:
        return False, _format_errors(errors), {}

    assert isinstance(benchmark, dict)
    required_root = ["benchmark_date", "primary_benchmark", "competitors", "design_differentiation"]
    for field in required_root:
        if field not in benchmark:
            errors.append(f"{path.as_posix()}: missing required field '{field}'")

    primary = benchmark.get("primary_benchmark")
    if isinstance(primary, dict):
        required_primary = ["name", "type", "cnv_number", "byma_member", "urls", "strengths", "weaknesses", "data_conventions"]
        for field in required_primary:
            if field not in primary:
                errors.append(f"{path.as_posix()}: primary_benchmark missing field '{field}'")
    else:
        errors.append(f"{path.as_posix()}: primary_benchmark must be object")

    competitors = benchmark.get("competitors")
    if not isinstance(competitors, list) or len(competitors) == 0:
        errors.append(f"{path.as_posix()}: competitors must be non-empty array")

    if errors:
        return False, _format_errors(errors), {}

    return True, "valid", {"benchmark": benchmark}


def validate_cross_file(project_root: Path, source_meta: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate consistency between source registry and puller modules."""
    errors: List[str] = []

    pullers_dir = project_root / "pullers"
    if not pullers_dir.exists():
        return False, f"{pullers_dir.as_posix()}: directory missing"

    puller_files = sorted(
        file.name
        for file in pullers_dir.glob("*.py")
        if file.name not in {"__init__.py", "base_puller.py"}
    )

    sources: List[Dict[str, Any]] = source_meta.get("sources", [])
    active_count = int(source_meta.get("active_count", 0))

    if active_count != len(puller_files):
        errors.append(
            f"active source count ({active_count}) != puller file count ({len(puller_files)}) in {pullers_dir.as_posix()}"
        )

    referenced_files: set[str] = set()
    for idx, source in enumerate(sources):
        puller_module = source.get("puller_module")
        if puller_module is None:
            continue
        if not isinstance(puller_module, str):
            errors.append(
                f"config/source_registry.json[{idx}]: puller_module must be string or null, got {type(puller_module).__name__}"
            )
            continue

        module_parts = puller_module.strip().split(".")
        if len(module_parts) < 2:
            errors.append(
                f"config/source_registry.json[{idx}]: invalid puller_module '{puller_module}' (expected package.module.Class)"
            )
            continue

        module_path = ".".join(module_parts[:-1])
        if not module_path.startswith("pullers."):
            errors.append(
                f"config/source_registry.json[{idx}]: puller_module '{puller_module}' must start with pullers."
            )
            continue

        file_name = f"{module_parts[-2]}.py"
        file_path = project_root / "pullers" / file_name
        if not file_path.exists():
            errors.append(
                f"config/source_registry.json[{idx}]: puller_module '{puller_module}' points to missing file {file_path.as_posix()}"
            )
        else:
            referenced_files.add(file_name)

    orphan_pullers = sorted(set(puller_files) - referenced_files)
    if orphan_pullers:
        for orphan in orphan_pullers:
            errors.append(f"pullers/{orphan}: orphan puller file not referenced by any source_registry entry")

    if errors:
        return False, _format_errors(errors)

    return True, f"{active_count} active sources = {len(puller_files)} puller files"


def validate_data_health(project_root: Path) -> Tuple[bool, str]:
    """Validate logs and data directories plus parseability of today's JSON files."""
    errors: List[str] = []
    logs_path = project_root / "logs" / "pull_log.jsonl"
    data_dir = project_root / "data"

    if not logs_path.exists():
        errors.append(f"{logs_path.as_posix()}: missing file")
    if not data_dir.exists():
        errors.append(f"{data_dir.as_posix()}: missing directory")

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_dir = data_dir / today_str
    parsed_files = 0

    if today_dir.exists():
        for json_file in sorted(today_dir.glob("*.json")):
            try:
                with open(json_file, "r", encoding="utf-8") as file_obj:
                    json.load(file_obj)
                parsed_files += 1
            except json.JSONDecodeError as exc:
                errors.append(f"{json_file.as_posix()}: invalid JSON ({exc})")
            except OSError as exc:
                errors.append(f"{json_file.as_posix()}: read error ({exc})")

    if errors:
        return False, _format_errors(errors)

    if today_dir.exists():
        return True, f"logs exist, today's data has {parsed_files} files"
    return True, "logs exist, no data directory for today yet"


def run_validator(project_root: Path | None = None) -> bool:
    """Run all validation categories and print report."""
    if project_root is None:
        project_root = Path(__file__).resolve().parent.parent

    source_ok, source_msg, source_meta = validate_source_registry(project_root / "config" / "source_registry.json")
    analyst_ok, analyst_msg, _ = validate_analyst_registry(project_root / "config" / "analyst_registry.json")
    benchmark_ok, benchmark_msg, _ = validate_competitive_benchmark(project_root / "config" / "competitive_benchmark.json")
    cross_ok, cross_msg = validate_cross_file(project_root, source_meta if source_ok else {})
    data_ok, data_msg = validate_data_health(project_root)

    categories = [
        ("source_registry", source_ok, source_msg),
        ("analyst_registry", analyst_ok, analyst_msg),
        ("competitive_benchmark", benchmark_ok, benchmark_msg),
        ("cross-file", cross_ok, cross_msg),
        ("data health", data_ok, data_msg),
    ]

    print("=== Config Validator ===")
    passed = 0
    for name, ok, message in categories:
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name}: {message}")
        if ok:
            passed += 1

    total = len(categories)
    if passed == total:
        print(f"=== {passed}/{total} PASSED ===")
        return True

    failures = total - passed
    print(f"=== {passed}/{total} PASSED - {failures} FAILURES ===")
    return False


def main() -> int:
    """CLI entry point."""
    ok = run_validator()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

