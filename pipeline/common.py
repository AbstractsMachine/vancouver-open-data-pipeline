"""
Shared plumbing for every script: configuration from the environment, the
BigQuery client, dataset names, output paths, JSON writing and a small logger.
Nothing here is specific to one dataset.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = Path(os.environ.get("VAN_CACHE_DIR", REPO_ROOT / "cache"))
OUTPUT_DIR = Path(os.environ.get("VAN_OUTPUT_DIR", REPO_ROOT / "outputs"))


def _load_dotenv() -> None:
    env = REPO_ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


_load_dotenv()

PROJECT_ID = os.environ.get("GCP_PROJECT", "")
RAW_DATASET = os.environ.get("VAN_RAW_DATASET", "vancouver_raw")
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "vancouver")
STAGING_DATASET = os.environ.get("VAN_STAGING_DATASET", f"{DBT_SCHEMA}_staging")
CORE_DATASET = os.environ.get("VAN_CORE_DATASET", f"{DBT_SCHEMA}_analytics")
MARTS_DATASET = os.environ.get("VAN_MARTS_DATASET", f"{DBT_SCHEMA}_marts")


def get_bigquery_client():
    """BigQuery client for GCP_PROJECT, credentials from
    GOOGLE_APPLICATION_CREDENTIALS or gcloud application-default login."""
    from google.cloud import bigquery

    if not PROJECT_ID:
        sys.exit("GCP_PROJECT is not set (copy .env.example to .env)")
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        adc = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
        if adc.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(adc)
    return bigquery.Client(project=PROJECT_ID, location=os.environ.get("BQ_LOCATION") or None)


def table(dataset: str, name: str) -> str:
    return f"{PROJECT_ID}.{dataset}.{name}"


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


class Logger:
    """Timestamped console logging with a closing summary."""

    def __init__(self, name: str):
        self.name = name
        self.t0 = time.time()
        self.counts = {"success": 0, "warning": 0, "error": 0}

    def _p(self, icon: str, msg: str, extra: str | None = None) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{stamp}] {icon} {msg}" + (f"  ({extra})" if extra else ""), flush=True)

    def header(self, title: str) -> None:
        print(f"\n=== {title} ===", flush=True)

    def section(self, title: str) -> None:
        print(f"\n--- {title}", flush=True)

    def info(self, msg: str, extra: str | None = None) -> None:
        self._p("·", msg, extra)

    def success(self, msg: str, extra: str | None = None) -> None:
        self.counts["success"] += 1
        self._p("ok", msg, extra)

    def warning(self, msg: str, extra: str | None = None) -> None:
        self.counts["warning"] += 1
        self._p("!!", msg, extra)

    def error(self, msg: str, extra: str | None = None) -> None:
        self.counts["error"] += 1
        self._p("ERROR", msg, extra)

    def data(self, label: str, value, unit: str = "") -> None:
        v = f"{value:,}" if isinstance(value, (int, float)) else str(value)
        self._p("·", f"{label}: {v}{(' ' + unit) if unit else ''}")

    def summary(self) -> None:
        print(f"\n{self.name}: done in {time.time() - self.t0:.1f}s — "
              f"{self.counts['success']} ok, {self.counts['warning']} warnings, {self.counts['error']} errors", flush=True)
