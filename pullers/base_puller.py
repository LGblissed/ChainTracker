"""Base abstraction for all data pullers."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict


class BasePuller(ABC):
    """Base class for all data pullers."""

    def __init__(self, source_id: str, source_name: str):
        """Create a puller with source metadata."""
        self.source_id = source_id
        self.source_name = source_name

    @abstractmethod
    def pull(self) -> Dict[str, Any]:
        """Execute the data pull and return a normalized result payload."""

    @staticmethod
    def utc_now_iso() -> str:
        """Return current UTC timestamp in ISO-8601 format ending with Z."""
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _project_root() -> str:
        """Return absolute path to the project root directory."""
        return os.path.dirname(os.path.dirname(__file__))

    def log_pull(self, result: Dict[str, Any]):
        """Append a compact pull log entry to logs/pull_log.jsonl."""
        log_entry = {
            "source_id": result.get("source_id", self.source_id),
            "pulled_at_utc": result.get("pulled_at_utc", self.utc_now_iso()),
            "status": result.get("status", "error"),
            "error_count": len(result.get("errors", [])),
            "errors": result.get("errors", []),
        }
        log_path = os.path.join(self._project_root(), "logs", "pull_log.jsonl")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as file_obj:
            file_obj.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    def save_daily(self, result: Dict[str, Any], date_str: str | None = None):
        """Save full pull payload to data/{date}/{source_id}.json."""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        data_dir = os.path.join(self._project_root(), "data", date_str)
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, f"{self.source_id}.json")
        with open(filepath, "w", encoding="utf-8") as file_obj:
            json.dump(result, file_obj, ensure_ascii=False, indent=2)

    def run(self) -> Dict[str, Any]:
        """Run the standard pull flow: pull -> log -> save."""
        try:
            result = self.pull()
        except Exception as exc:  # Defensive fallback for unexpected errors.
            result = {
                "source_id": self.source_id,
                "pulled_at_utc": self.utc_now_iso(),
                "status": "error",
                "data": {},
                "errors": [f"Unhandled pull error: {exc}"],
                "raw_response_snippet": "",
            }
        self.log_pull(result)
        self.save_daily(result)
        return result
