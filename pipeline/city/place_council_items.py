#!/usr/bin/env python3
"""Place every council item that names an address on the parcel it names.

    python3 pipeline/city/place_council_items.py [--dry-run]
        -> raw.ca_vancouver_council_item_places  (one row per item × resolved parcel)

WHAT A COUNCIL ITEM CITES. Vancouver's voting record carries one free-text
`agenda_description` per item — "3. Rezoning: 607-621 West 28th Avenue",
"CD-1 Rezoning and Heritage Designation - 1872 Parker Street (Brookhouse
Residence)". the unit a decision attaches to is whatever the
decision itself cites; here it is a civic ADDRESS, and the City publishes the
register that turns an address into a parcel (`property-addresses`:
civic_number + std_street → pcoord = the parcel's tax coordinate).

THE RULES, IN ORDER, AND NOTHING FUZZY:
  1. find "<number>[-<number>] [West|East] <name> <street type>" in the text;
  2. write the street the way the register writes it — West → W, Avenue → AV,
     Street → ST, "28th" → 28TH — using the register's own suffix spellings;
  3. an address is PLACED when (civic number, street) exists in the register.
     A range "607-621" places every register address on that street whose
     number lies in the range;
  4. otherwise, and only on the SAME street, the address is placed on the
     nearest registered number in the same HUNDRED BLOCK (rule `hundred_block`).
     This is the case the rezoning record is full of: a site assembled from
     lots whose old civic numbers left the register when the lots were
     consolidated ("5021-5079 Quebec Street"). The hundred block is the City's
     own addressing unit (public-streets `hblock`) and the map's unit is the
     block, so this places the decision on the block it names — and the row
     says which rule placed it.
No fuzzy street matching: measured that a looser street match on
another city's decision record placed half its new matches on the wrong street, silently. An item
this cannot place stays unplaced and is counted, never guessed.

Text repair: the export carries UTF-8 read as cp1252 ("â€“" for an en dash);
repaired before matching, and the repaired text is what the stg model uses too.

Writes to raw, like every measured/derived source (measure_building_heights.py).
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # pipeline/
from common import PROJECT_ID, get_bigquery_client, RAW_DATASET, CORE_DATASET, MARTS_DATASET  # noqa: E402

ANALYTICS = CORE_DATASET
OUT_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.ca_vancouver_council_item_places"

SUFFIX = {
    "street": "ST", "st": "ST", "avenue": "AV", "ave": "AV", "av": "AV",
    "drive": "DRIVE", "dr": "DRIVE", "road": "ROAD", "rd": "ROAD",
    "crescent": "CRESCENT", "place": "PLACE", "boulevard": "BOULEVARD", "blvd": "BOULEVARD",
    "way": "WAY", "court": "COURT", "mews": "MEWS", "lane": "LANE", "highway": "HIGHWAY",
    "diversion": "DIVERSION", "connector": "CONNECTOR", "circle": "CIRCLE",
}
BARE = {"broadway": "BROADWAY", "kingsway": "KINGSWAY"}

ADDR = re.compile(
    r"(?<![\d$.])(\d{1,5})(?:\s*[-–]\s*(\d{1,5}))?\s+"
    r"(?:(southwest|southeast|northwest|west|east|sw|se|nw|w\.?|e\.?)\s+)?"
    r"((?:[a-z0-9']+\s+){0,3}?)"
    r"(street|st|avenue|ave|av|drive|dr|road|rd|crescent|place|boulevard|blvd|way|court|mews|lane|highway|"
    r"diversion|connector|circle|broadway|kingsway)\b\.?",
    re.IGNORECASE,
)


def repair(text: str) -> str:
    if not text:
        return text
    if "â" in text or "Ã" in text:
        try:
            return text.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text
    return text


def candidates(text: str):
    """[(civic_from, civic_to, std_street)] in the register's spelling."""
    out = []
    for m in ADDR.finditer(text):
        n0, n1, direc, name, typ = m.group(1), m.group(2), m.group(3), m.group(4).strip(), m.group(5).lower()
        d = ""
        if direc:
            dl = direc.lower().rstrip(".")
            d = {"southwest": "SW ", "sw": "SW ", "southeast": "SE ", "se": "SE ", "northwest": "NW ", "nw": "NW ",
                 "west": "W ", "w": "W ", "east": "E ", "e": "E "}[dl]
        if typ in BARE:
            if name:          # "123 West Broadway" has no name before the type
                continue
            street = f"{d}{BARE[typ]}"
        else:
            if not name:
                continue
            words = name.upper().split()
            # a year or a count before the street is not a civic number
            if words and words[0] in {"AND", "TO", "OF", "THE", "AT", "ON"}:
                continue
            words = [re.sub(r"^(\d+)(ST|ND|RD|TH)$", r"\1\2", w) for w in words]
            street = f"{d}{' '.join(words)} {SUFFIX[typ]}"
        a = int(n0)
        b = int(n1) if n1 else a
        if b < a:
            a, b = b, a
        if b - a > 400:
            continue
        out.append((a, b, street))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    c = get_bigquery_client()
    items = c.query(f"""
        SELECT item_key, ANY_VALUE(agenda_description) AS agenda_description
        FROM `{PROJECT_ID}.{ANALYTICS}.core_ca_vancouver_votes`
        GROUP BY item_key""").to_dataframe()
    reg = c.query(f"""
        SELECT SAFE_CAST(civic_number AS INT64) AS civic, UPPER(TRIM(std_street)) AS street, pcoord
        FROM `{PROJECT_ID}.{RAW_DATASET}.ca_vancouver_property_addresses`
        WHERE civic_number IS NOT NULL AND std_street IS NOT NULL AND pcoord IS NOT NULL""").to_dataframe()
    by_street: dict[str, dict[int, set]] = defaultdict(lambda: defaultdict(set))
    for civic, street, pc in reg.itertuples(index=False):
        if pd.notna(civic):
            by_street[street][int(civic)].add(pc)

    rows, stats = [], {"items": len(items), "with_candidate": 0, "placed": 0, "candidates": 0, "unmatched_candidates": 0}
    unmatched_examples = []
    for key, desc in items.itertuples(index=False):
        text = repair(desc or "")
        cands = candidates(text)
        if not cands:
            continue
        stats["with_candidate"] += 1
        placed_any = False
        for a, b, street in cands:
            stats["candidates"] += 1
            nums = by_street.get(street)
            hits = []
            if nums:
                hits = [(n, pc) for n, pcs in nums.items() if a <= n <= b for pc in pcs]
            rule = "exact" if a == b else "range"
            if not hits and nums:
                for want in sorted({a, b}):
                    same = [n for n in nums if n // 100 == want // 100]
                    if same:
                        n = min(same, key=lambda k: (abs(k - want), k))
                        hits += [(n, pc) for pc in nums[n]]
                if hits:
                    rule = "hundred_block"
            if not hits:
                stats["unmatched_candidates"] += 1
                if len(unmatched_examples) < 25:
                    unmatched_examples.append((f"{a}-{b} {street}", text[:90]))
                continue
            placed_any = True
            for n, pc in sorted(set(hits)):
                rows.append({"item_key": key, "civic_from": a, "civic_to": b, "std_street": street,
                             "civic_number": n, "tax_coord": pc, "rule": rule,
                             "agenda_description_repaired": text})
        stats["placed"] += int(placed_any)
    print(stats)
    print("unmatched examples:")
    for e in unmatched_examples:
        print("  ", e)
    df = pd.DataFrame(rows).drop_duplicates(subset=["item_key", "tax_coord"])
    print(f"rows (item × parcel): {len(df)}")
    if args.dry_run:
        return 0
    df["_synced_at"] = datetime.now(timezone.utc)
    from google.cloud import bigquery
    c.load_table_from_dataframe(df, OUT_TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
    print(f"loaded → {OUT_TABLE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
