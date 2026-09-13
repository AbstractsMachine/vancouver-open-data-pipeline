#!/usr/bin/env python3
"""Parse the SOFI's FIVE YEAR STATISTICAL REVIEW into long rows.

    python3 pipeline/finance/parse_sofi_review.py [--load]
        -> raw.ca_vancouver_sofi_review
           (report_year, table_name, section, section_key, label, label_key, fiscal_year, value, is_total, page)

WHAT IT IS. From 2019 the City's Statement of Financial Information carries its
audited statements and a five-year statistical review: the consolidated
statement of operations (revenue by source, expenses by function), expenses by
object, capital additions, debt (with the population the City divides by),
reserves, and taxation (rates, the property-tax share by class, taxes
collected for the City and for other authorities). Seven reports (2019–2025)
cover fiscal 2015–2025, and each restates the years before it — so the rows
keep `report_year`, and the core model takes the LATEST report's value for
each year (a restated figure supersedes the one first printed).

HOW. A review page is one whose running header starts "FIVE YEAR". A table is
a title line (the layout changed three times: "FIVE YEAR - CONSOLIDATED
STATEMENT OF OPERATIONS" 2019–2022, no titles at all in 2023, "CONSOLIDATED
STATEMENT OF OPERATIONS" 2024–2025; the 2023 tables are told apart by the
section only one of them has — Financial assets, Revenues, Long Term Debt…), a
header of five years (the text layer sometimes splits one: "2 019"), then rows
read from the RIGHT: trailing numbers ($ and * skipped, "-" = 0, parentheses
negative), the rest is the label. A line with a label and no numbers opens a
section; a row with numbers and no label is that section's total. Rows with
fewer than five numbers (continuity sub-tables, page numbers) are counted as
skipped, never guessed. Labels are keyed letters-only (some pages are
letter-spaced: "F in a n cia l Sta biliza tion"). Units are the table's own:
most are $000s; population is persons; per-capita debt is dollars; tax rates are
per $1,000 of assessment; class shares are %.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import CACHE_DIR, REPO_ROOT  # noqa: E402
ROOT = REPO_ROOT
CACHE = CACHE_DIR / "sofi"

NUMTOK = re.compile(r"^\(?-?[\d,]*\d(?:\.\d+)?\)?$")
YEAR = re.compile(r"^(20\d\d)\*?$")

# a title (or, in the 2023 report, which prints none, a section that only one
# table has) → the table it opens. Keys are letters only, lower case, so the
# letter-spaced text layer of some pages ("F in a n cia l Sta biliza tion") matches.
TITLES = [
    ("statementoffinancialposition", "financial_position"),
    ("statementofoperations", "operations"),
    ("expensesbyobject", "expenses_by_object"),
    ("acquisitionoftangiblecapitalassets", "capital_additions"),
    ("reviewdebt", "debt"), ("fiveyeardebt", "debt"),
    ("reviewtaxation", "taxation"), ("fiveyeartaxation", "taxation"),
    ("reserves", "reserves"),
]
SECTION_OPENS = {
    "financialassets": "financial_position",
    "revenues": "operations",
    "longtermdebt": "debt",
    "financialstabilization": "reserves",
    "propertyassessment": "taxation",
}


def key(text: str) -> str:
    return re.sub(r"[^a-z]", "", text.lower())


def value(tok: str) -> float:
    neg = tok.startswith("(") and tok.endswith(")")
    v = float(re.sub(r"[(),]", "", tok))
    return -v if neg else v


def split_row(line: str):
    """(label, [numeric tokens]) — numbers read from the right; '$' and '*' are skipped, '-' is zero."""
    toks = line.split()
    nums: list[str] = []
    i = len(toks)
    while i > 0:
        t = toks[i - 1]
        if t in ("$", "*"):
            i -= 1
            continue
        if t == "-" or NUMTOK.match(t.lstrip("$")):
            nums.insert(0, "0" if t == "-" else t.lstrip("$"))
            i -= 1
            continue
        break
    return " ".join(toks[:i]).rstrip(" $*"), nums


def parse_report(report_year: int):
    txt = subprocess.run(["pdftotext", "-layout", str(CACHE / f"{report_year}-statement.pdf"), "-"],
                         capture_output=True, text=True, check=True).stdout
    rows, skipped = [], 0
    for pno, pg in enumerate(txt.split("\f"), start=1):
        lines = [l for l in pg.splitlines() if l.strip()]
        head = key(" ".join(lines[:2]))
        if not head.startswith("fiveyear"):
            continue
        table = section = years = None
        totals_seen: set = set()
        for lno, line in enumerate(lines):
            k = key(line)
            if "annualfinancialreport" in k or k.startswith("yearsended") or k.startswith("yearended") or k.startswith("tabularamounts"):
                continue
            digits = re.sub(r"[\s*]", "", line)
            if digits.isdigit() and len(digits) == 20:          # "2025 2024 2023* ..." or "2 019  2018 ..."
                ys = [int(digits[i:i + 4]) for i in range(0, 20, 4)]
                if all(ys[i] - ys[i + 1] == 1 for i in range(4)):
                    years = ys
                continue
            if k and set(re.findall(r"[a-z]+", line.lower())) == {"restated"}:
                continue
            label, nums = split_row(line)
            lk = key(label)
            if not nums:
                alpha = [c for c in line if c.isalpha()]
                titled = k.startswith("fiveyear") or (alpha and sum(c.isupper() for c in alpha) >= 0.8 * len(alpha))
                opened = next((t for frag, t in TITLES if frag in k), None) if titled else None
                if opened is None and table is None:
                    opened = next((t for frag, t in SECTION_OPENS.items() if k.startswith(frag)), None)
                    if opened is None and k == "expenses":
                        opened = "expenses_by_object"
                if opened is None and k.startswith("longtermdebt"):
                    opened = "debt"
                if opened and opened != table:
                    table, section = opened, None
                if opened is None or not titled:
                    section = re.sub(r"\s+", " ", line.strip())
                continue
            if lk.startswith("population"):
                table, section = "debt", None
            if lk == "capitaladditions":
                table, section = "capital_additions", None
            if not years or not table:
                skipped += 1
                continue
            if len(nums) < 5:
                if label and len(nums) == 1 and nums[0].isdigit() and int(nums[0]) < 10:
                    section = re.sub(r"\s+", " ", line.strip())      # "Residential - class 1" heads a tax-rate block
                else:
                    skipped += 1
                continue
            if len(nums) > 5:
                # extra leading numbers belong to the label ("Class 1"), unless
                # there is no label: then it is a rotated page's folio merged
                # onto a total line ("44   1,177,437 ...")
                label = (label + " " + " ".join(nums[:-5])).strip() if label else ""
                nums = nums[-5:]
            # "(Note 15)" reads as a trailing number; it is a reference, not a value
            label = re.sub(r"\s*\((?:Note|note)\s*\d+\)", "", label)
            # a second unlabelled total under the same section is the table's
            # grand total (reserves: the last group's subtotal, one labelled
            # reserve, then the sum of every group)
            grand = False
            if not label:
                grand = (table, section) in totals_seen
                totals_seen.add((table, section))
            for fy, v in zip(years, nums):
                rows.append({"report_year": report_year, "table_name": table, "section": section,
                             "label": re.sub(r"\s+", " ", label) if label else ("Grand total" if grand else None),
                             "label_key": key(label) or ("grandtotal" if grand else None), "line_no": lno,
                             "section_key": key(section) if section else None,
                             "fiscal_year": fy, "value": value(v), "is_total": not label, "page": pno})
    return rows, skipped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--load", action="store_true")
    args = ap.parse_args()
    allrows = []
    for ry in range(2019, 2026):
        rows, skipped = parse_report(ry)
        tables = sorted({r["table_name"] for r in rows})
        print(f"{ry}: {len(rows)} values, {skipped} lines skipped; tables {tables}")
        allrows += rows
    for ry, tb in ((2019, "operations"), (2023, "expenses_by_object"), (2023, "debt"), (2021, "reserves")):
        print(f"  -- {ry} {tb}")
        for r in allrows:
            if r["report_year"] == ry and r["table_name"] == tb and r["fiscal_year"] == min(x["fiscal_year"] for x in allrows if x["report_year"] == ry):
                print("    ", r["section"], "|", r["label"], r["value"])
    if args.load:
        import pandas as pd
        from google.cloud import bigquery
        from common import PROJECT_ID, get_bigquery_client, RAW_DATASET, CORE_DATASET, MARTS_DATASET  # noqa: E402
        df = pd.DataFrame(allrows)
        df["_synced_at"] = datetime.now(timezone.utc)
        get_bigquery_client().load_table_from_dataframe(
            df, f"{PROJECT_ID}.{RAW_DATASET}.ca_vancouver_sofi_review",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
        print(f"loaded {len(df)} → raw.ca_vancouver_sofi_review")
    return 0


if __name__ == "__main__":
    sys.exit(main())
