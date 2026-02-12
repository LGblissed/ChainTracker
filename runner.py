"""Daily runner for Argentina Chain Tracker."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

from pullers.bcra_reserves import BCRAReservesPuller
from pullers.fx_rates import FXRatesPuller
from pullers.us_yields import USYieldsPuller
from scripts.validate_config import run_validator


def _parse_args() -> argparse.Namespace:
    """Parse CLI flags for validation and execution modes."""
    parser = argparse.ArgumentParser(description="Argentina Chain Tracker runner.")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run only configuration/data validation and exit.",
    )
    parser.add_argument(
        "--validate-first",
        action="store_true",
        help="Run validator before pulling data. Abort pull run on validation failure.",
    )
    return parser.parse_args()


def _run_pulls() -> int:
    """Run all configured pullers sequentially and print summary."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"=== Argentina Chain Tracker - {date_str} ===")

    pullers = [
        USYieldsPuller(),
        BCRAReservesPuller(),
        FXRatesPuller(),
    ]

    results = []
    for puller in pullers:
        print(f"Pulling: {puller.source_name}...")
        try:
            result = puller.run()
            status = result.get("status", "unknown")
            print(f"  Status: {status}")
            if result.get("errors"):
                for err in result["errors"]:
                    print(f"  Warning: {err}")
            results.append(result)
        except Exception as exc:
            print(f"  FATAL ERROR: {exc}")
            results.append(
                {
                    "source_id": puller.source_id,
                    "pulled_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "status": "fatal_error",
                    "errors": [str(exc)],
                }
            )

    ok_count = sum(1 for item in results if item.get("status") == "ok")
    issue_count = len(results) - ok_count
    print(f"\n=== Complete: {ok_count} ok, {issue_count} issues ===")
    return 0


def main() -> int:
    """Entry point with optional validation gate flags."""
    args = _parse_args()
    project_root = Path(__file__).resolve().parent

    if args.validate:
        return 0 if run_validator(project_root=project_root) else 1

    if args.validate_first:
        if not run_validator(project_root=project_root):
            print("Validation failed. Pull run aborted.")
            return 1

    return _run_pulls()


if __name__ == "__main__":
    sys.exit(main())
