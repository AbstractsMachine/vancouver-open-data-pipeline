#!/usr/bin/env python3
"""Fetch Vancouver's Statements of Financial Information from the Internet Archive.

    python3 pipeline/finance/fetch_sofi.py
        -> cache/sofi/<file>.pdf            (gitignored)
        -> dbt/seeds/seed_ca_vancouver_sofi_documents.csv (committed)

WHY THE ARCHIVE. The City publishes its SOFI (BC Financial Information Act:
audited statements, the schedules of debts, remuneration, payments to
suppliers over $25,000, and grants) as PDFs on vancouver.ca, and that host
refuses scripted requests. The Internet Archive's Wayback Machine holds the
same files, byte for byte, from 2011 to 2025 — found through its CDX index
(`vancouver.ca/files/cov/` prefix). Every document is fetched from the
`id_` endpoint (the archived bytes, no Wayback banner) and recorded with its
capture timestamp, original URL and SHA-256, so the manifest is enough to get
exactly these bytes again.

WHICH FILES. The main city statement for each fiscal year. For 2017 and 2018
the supplier and grant schedules were published as a separate
"supply-management" file beside the statement; both are fetched and both are
recorded (`part`). 2019 is published under a different file name.
"""
from __future__ import annotations

import csv
import hashlib
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import CACHE_DIR, REPO_ROOT  # noqa: E402
ROOT = REPO_ROOT
CACHE = CACHE_DIR / "sofi"
MANIFEST = ROOT / "dbt" / "seeds" / "seed_ca_vancouver_sofi_documents.csv"


# fiscal_year, part, capture timestamp, original URL — from the CDX index, 2026-09-13
DOCS = [
    (2011, "statement", "20150910080503", "http://vancouver.ca/files/cov/2011-statement-of-financial-information.pdf"),
    (2012, "statement", "20150910080614", "http://vancouver.ca/files/cov/2012-statement-of-financial-information.pdf"),
    (2013, "statement", "20141121124755", "http://vancouver.ca/files/cov/2013-statement-of-financial-information.pdf"),
    (2014, "statement", "20150910080426", "http://vancouver.ca/files/cov/2014-statement-of-financial-information.pdf"),
    (2015, "statement", "20160715044038", "http://vancouver.ca/files/cov/2015-statement-of-financial-information.pdf"),
    (2016, "statement", "20170421042219", "http://vancouver.ca/files/cov/2016-statement-of-financial-information.pdf"),
    (2017, "statement", "20180711235916", "https://vancouver.ca/files/cov/2017-statement-of-financial-information.pdf"),
    (2017, "supply-management", "20180712034403", "https://vancouver.ca/files/cov/2017-statement-of-financial-information-supply-management.pdf"),
    (2018, "statement", "20190723011646", "https://vancouver.ca/files/cov/city-of-vancouver-statement-of-financial-information-december-31-2018.pdf"),
    (2018, "supply-management", "20190723074512", "https://vancouver.ca/files/cov/2018-statement-of-financial-information-supply-management.pdf"),
    (2019, "statement", "20200404015244", "https://vancouver.ca/files/cov/statement-financial-information-2019.pdf"),
    (2020, "statement", "20210706214130", "https://vancouver.ca/files/cov/2020-statement-of-financial-information.pdf"),
    (2021, "statement", "20220705191421", "https://vancouver.ca/files/cov/2021-statement-of-financial-information.pdf"),
    (2022, "statement", "20230405164120", "https://vancouver.ca/files/cov/2022-statement-of-financial-information.pdf"),
    (2023, "statement", "20240417230456", "https://vancouver.ca/files/cov/2023-statement-of-financial-information.pdf"),
    (2024, "statement", "20250410190235", "https://vancouver.ca/files/cov/2024-statement-of-financial-information.pdf"),
    (2025, "statement", "20260713042147", "https://vancouver.ca/files/cov/2025-statement-of-financial-information.pdf"),
]


def main() -> int:
    CACHE.mkdir(parents=True, exist_ok=True)
    rows = []
    for year, part, ts, url in DOCS:
        name = f"{year}-{part}.pdf"
        path = CACHE / name
        wb = f"https://web.archive.org/web/{ts}id_/{url}"
        if not path.exists() or path.stat().st_size < 10000:
            for attempt in range(4):
                try:
                    req = urllib.request.Request(wb, headers={"User-Agent": "vancouver-open-data-pipeline/1.0"})
                    with urllib.request.urlopen(req, timeout=180) as r:
                        blob = r.read()
                    if blob[:4] != b"%PDF":
                        raise RuntimeError(f"not a PDF: {blob[:40]!r}")
                    path.write_bytes(blob)
                    break
                except Exception as e:  # the archive rate-limits bursts; back off and retry
                    print(f"  {name}: {e} (retry {attempt + 1})", flush=True)
                    time.sleep(5 * (attempt + 1))
            else:
                print(f"FAILED {name}", flush=True)
                return 1
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        rows.append({"fiscal_year": year, "part": part, "wayback_timestamp": ts, "original_url": url,
                     "wayback_url": wb, "file": name, "bytes": path.stat().st_size, "sha256": sha})
        print(f"{name}  {path.stat().st_size:>9,}  {sha[:12]}", flush=True)
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"manifest → {MANIFEST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
