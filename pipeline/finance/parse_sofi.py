#!/usr/bin/env python3
"""Parse the SOFI schedules of payments to suppliers and grants, checked to the dollar.

    python3 pipeline/finance/parse_sofi.py [--load] [--years 2011-2025]
        -> raw.ca_vancouver_sofi_suppliers  (fiscal_year, name, amount_cad, is_individual, page)
        -> raw.ca_vancouver_sofi_grants     (fiscal_year, category, name, amount_cad, is_individual, page)
        -> raw.ca_vancouver_sofi_controls   (fiscal_year, schedule, label, amount_cad, parsed_sum_cad, delta_cad)

THE DOCUMENT. Each year's SOFI (fetched from the Internet Archive by
fetch_sofi.py) prints the supplier schedule as a two-column table of
"Name   Amount" pairs over many pages, closing with the City's own control rows
("Vendors > $25,000", "Vendors < $25,000", "Total"), and the grant schedule as
categories (Business Improvement Associations, Cultural, Community, Childcare,
Affordable Housing, Other), each closing on its own total.

THE RULE THAT MAKES IT TRUSTWORTHY. A parsed schedule is published only if the
sum of its rows equals the total the City printed, to the dollar; every year's
comparison is written to `ca_vancouver_sofi_controls` and the dbt test fails on
any nonzero delta. A long name that wraps onto a second line is rejoined to its
amount; a row that cannot be read is reported, not dropped silently.

INDIVIDUALS. The schedules pay some private persons, printed "Surname, Given"
(e.g. a sole contractor or an artist). `is_individual` flags that shape; the
row keeps its amount for every total and its NAME NEVER LEAVES THE WAREHOUSE —
marts aggregate them as "individuals (names not published here)". No output is built for a person.
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

AMOUNT = r"\$?\s*\(?-?\d{1,3}(?:,\d{3})+\)?|\$?\s*\(?-?\d+\)?"
# a thousands-grouped amount: every supplier row is over $25,000, so its amount
# always carries a comma — and a digit inside a NAME ("(2011) Ltd", "Local 118")
# never does. Column detection uses only this form.
GROUPED = r"\$?\s*-?\d{1,3}(?:,\d{3})+"
AMT_TAIL = re.compile(rf"^(.*?\S)\s+({AMOUNT})\s*$")
AMT_TAIL_GROUPED = re.compile(rf"^(.*?\S)\s+({GROUPED})\s*$")
ORG = re.compile(r"\b(ltd|limited|inc|incorporated|corp|corporation|co|company|llp|lp|llc|ulc|plc|society|societies|association|assoc|"
                 r"foundation|trust|bank|club|council|centre|center|church|university|college|school|board|"
                 r"city|province|provincial|government|minister|ministry|district|region|regional|authority|agency|service|services|group|partners|partnership|"
                 r"consult\w*|design|architects?|engineering|construction|contracting|contractors?|systems|solutions|technologies|technology|"
                 r"canada|canadian|vancouver|british columbia|bc|holdings|enterprises|industries|products|supply|supplies|equipment|"
                 r"hotel|restaurant|cafe|studio|studios|productions|media|theatre|festival|museum|gallery|library|network|"
                 r"institute|union|federation|coalition|alliance|cooperative|co-op|housing|properties|property|realty|"
                 r"management|investments|developments?|ventures|international|global|national|world|"
                 r"in trust|barristers|solicitors|law|lawyers|notary|associates|store|stores|market|motors|auto|electric|"
                 r"plumbing|roofing|paving|landscaping|excavating|trucking|rentals?|leasing|insurance|pension|plan|fund|"
                 r"collective|project|projects|program|house|home|homes|care|health|medical|clinic|hospital|"
                 r"dental|pharmacy|farms?|foods?|catering|printing|signs?|lighting|water|energy|power|gas|oil|fuel|"
                 r"canada post|receiver general|metro|translink|police|fire|parks?|recreation|"
                 r"architecture|brewing|brewery|communications?|intelligence|sales|gravel|rock|refrigeration|shop|"
                 r"dealership|gp|efficiency|motorsports|telecom|fees|commercial|industrial|mechanical|electrical|"
                 r"environmental|geotechnical|surveying|surveys|appraisal|research|analytics|software|data|digital|"
                 r"graphics|publishing|press|news|radio|entertainment|events|sports|fitness|security|cleaning|"
                 r"janitorial|maintenance|repair|towing|transport|transportation|logistics|courier|moving|storage|"
                 r"hardware|lumber|steel|concrete|asphalt|glass|paint|flooring|doors|pumps|controls|instruments|"
                 r"safety|uniforms|apparel|office|furniture|coffee|bakery|kitchen|bistro|pub|grill|tire|tires|"
                 r"trucks?|marine|aviation|fleet|parts|tools|machinery|labs?|laboratory|scientific|testing|"
                 r"planning|landscape|arborists?|nursery|nurseries|recycling|waste|disposal|environment|"
                 r"learning|training|education|academy|arts|dance|music|film|theater|community|neighbourhood|"
                 r"dba|division|associates|partners|lp|trustee|estate|resources|capital|financial|credit|leasing|"
                 r"hydro|telus|rogers|bell|shaw|amazon|staples|microsoft|oracle|ibm|cisco|google|apple|"
                 r"hotels|inn|drugs|rent|car|cars|cycling|computers|chambers|fuels|athletic|wear|embroidery|bookstore|"
                 r"crane|rigging|foodservice|promotions|cabling|soc|chevrolet|ford|dodge|gmc|toyota|costco|chevron|"
                 r"facebook|linkedin|commissionaires|litigation|saddlery|innovations|lease|township|corporate|"
                 # abbreviations and kinds of body the schedules print without a legal form
                 r"dist|assn|bia|business improvement|band|nation|army|ctr|div|itf|comm\.|"
                 r"engineers|masonry|motorcycles|networks|supermarkets?|fittings|hose|propane|pipe|trailers?|"
                 r"traffic control|transfer|prepaid|field operations|family place|cutting|secured|"
                 # trade names with no marker at all, large enough to matter (measured 2026-09-13)
                 r"blue cross|e-?comm|grand & toy|moneris|primecorp|microserve|finning|united we can|embers|"
                 r"pitney ?works|datacore|vantage point|mosaic|westpro|rocky mountain phoenix|kenworth|western star|"
                 r"mitsubishi|enviro-vac|pop-a-lock|salvation army|horsman)\b", re.I)
# people: a comma between a surname and a given name, or a personal title, is always a person
PERSON = re.compile(r"^[A-Z][A-Za-z'\-]+(?: [A-Z][A-Za-z'\-]+)?, (?:Dr\.? |Mr\.? |Ms\.? |Mrs\.? )?[A-Z][A-Za-z'\-]+(?: [A-Z]\.?)?$|\b(Dr|Mr|Mrs|Ms)\.\s", re.I)


def amount(tok: str) -> int:
    neg = "(" in tok or "-" in tok
    v = int(re.sub(r"[^\d]", "", tok) or 0)
    return -v if neg else v


# Marker words that are also common surnames ("Sarah Bell", "Jiwon Park", "Kevin Law"). Alone at the end of a name
# shaped like "Given [I.] Surname" they mark a person, not a firm; "Bell Canada" or "Park Board" keep their other marker.
SURNAME_MARKERS = frozenset(
    "bell rogers shaw ford park parks law house home homes glass power rock fire market water car care fleet steel inn "
    "gas oil store shop press news music film dance plan fund apple kitchen paint tools parts data fuel rent lease sales".split()
)
GIVEN_SURNAME = re.compile(r"^[A-Z][a-z'\-]+(?: [A-Z]\.?| [A-Z][a-z'\-]+)? ([A-Z][a-z'\-]+)$")


def _surname_only_marker(name: str) -> bool:
    """The name is shaped like "Given [I.] Surname" and its only organisation marker is that surname."""
    shape = GIVEN_SURNAME.match(name)
    if not shape or shape.group(1).lower() not in SURNAME_MARKERS:
        return False
    return ORG.search(name[: shape.start(1)]) is None


def is_individual(name: str) -> bool:
    """CONSERVATIVE: a payee is published by name only when the name carries an
    organisation marker (a legal form, a kind of body, a trade word) or a digit.
    Everything else — "Surname, Given", "Given Surname", or a trade name with
    no marker — is treated as possibly a person and kept out of every export.
    A marker that is also a common surname ("Sarah Bell") does not count."""
    n = name.strip()
    if PERSON.search(n) and not re.search(r"\b(ltd|inc|corp|llp|law corporation|in trust|society|association)\b", n, re.I):
        return True
    if _surname_only_marker(n):
        return True
    if re.search(r"\d", n) or ORG.search(n):
        return False
    if re.fullmatch(r"[A-Z0-9&.\- ]{2,}", n) and len(n.replace(" ", "")) <= 6:   # acronyms: ICBC, BCIT
        return False
    return True


# Typos in the City's own PDFs, repaired before parsing — each only where the
# repaired figure is the one that makes the printed total add up, and each
# listed here with that evidence (nothing else is corrected):
TEXT_REPAIRS = {
    # "On Main Gallery (On The Cutting Edge Productions)   15,0000": an extra
    # zero. Read as 15,000, the Cultural grants add up to the printed
    # $12,689,325 exactly; unrepaired the amount is lost and the name merges
    # into the next row.
    2018: [("15,0000", "15,000")],
}

# An amount whose leading digits the PDF itself cuts off (",541,678" — 2021,
# Aramark Canada Ltd): the row is kept with NO amount (amount_text keeps what is
# printed) rather than guessed, and its name no longer swallows the next row.
TRUNCATED_TAIL = re.compile(r"^(.*?[A-Za-z].*?\S)\s+(,\d{3}(?:,\d{3})*)\s*$")


def repair(year: int, pgs: list[str]) -> list[str]:
    out = []
    for pg in pgs:
        for bad, good in TEXT_REPAIRS.get(year, []):
            pg = pg.replace(bad, good)
        out.append(pg)
    return out


def pages(pdf: Path) -> list[str]:
    txt = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], capture_output=True, text=True, check=True).stdout
    return txt.split("\f")


CONTROL = re.compile(r"^(vendors?|suppliers?|payments?)\s*(over|under|[<>])|^total\s*(-|–|\$|$)|^total amendment|^revised total|^previously reported|^grand total", re.I)
SKIP_LINE = re.compile(r"STATEMENT OF FINANCIAL INFORMATION|SUPPLIERS OF GOODS|^\s*Name\s+Amount|CITY OF VANCOUVER\s*$|^\s*GRANTS", re.I)


def column_split(lines: list[str]) -> int | None:
    """Where the right-hand column starts on this page: the start of the text that
    follows a line's first amount, read off lines that carry TWO grouped amounts
    (a left row and a right row) — one such line fixes the split exactly, so a
    short last page is split by its own layout, not the schedule's."""
    from collections import Counter
    starts = Counter()
    for s in lines:
        m = re.match(rf"^(\s*\S.*?\S)\s+({GROUPED})\s+(\S.*?)\s+({GROUPED})\s*$", s)
        if m and re.search(r"[A-Za-z]", m.group(1)) and re.search(r"[A-Za-z]", m.group(3)):
            starts[m.start(3)] += 1
    if not starts:
        return None
    pos, _ = starts.most_common(1)[0]
    return min(k for k in starts if abs(k - pos) <= 2)


def cells(lines: list[str], fallback: int | None = None):
    """(column index, text) cells of a two-column page, in reading order per column.
    A page with too few two-column lines to find its own split (a schedule's last
    page) takes the split measured over the whole schedule (`fallback`)."""
    col = column_split(lines)
    if col is None:
        col = fallback
    out = []
    for s in lines:
        if not s.strip() or SKIP_LINE.search(s):
            continue
        if col is not None and len(s) > col:
            # never cut inside a name: cut at the end of the nearest GAP (two or
            # more spaces) at or left of the column — a right-hand name that
            # starts a few characters early keeps its first word
            k = col
            lo = max(0, col - 14)
            gaps = []
            for mm in re.finditer(r"\s{2,}", s[lo:col + 1]):
                g0, g1 = mm.start() + lo, mm.end() + lo
                before = s[:g0]
                # a column ends after its amount, or after a name that has none
                if before.rstrip().endswith("$"):
                    continue          # "$   1,005,874": the gap inside an amount
                if re.match(rf"\$?\s*{GROUPED}", s[g1:]):
                    continue          # the gap BEFORE a left row's amount is not the column edge
                if re.search(rf"{GROUPED}\s*$|\d\s*$", before) or not re.search(GROUPED, before):
                    gaps.append(g1)
            if gaps:
                k = max(gaps)
            else:
                while k > 0 and not s[k - 1].isspace() and not s[k].isspace():
                    k -= 1
            left, right = s[:k], s[k:]
            # a long left-hand name pushes its amount past the cut: an amount that
            # OPENS the right half belongs to the left row when the left has none
            mm = re.match(rf"^\s*({GROUPED})(?=\s{{2,}}|\s*$)", right)
            if mm and not re.search(rf"{GROUPED}\s*$", left):
                left = left.rstrip() + "  " + mm.group(1)
                right = right[mm.end():]
        else:
            left, right = s, ""
        for ci, half in ((0, left), (1, right)):
            if half.strip():
                out.append((ci, half.strip()))
    return out


def read_pairs(lines: list[str], fallback: int | None = None, grouped: bool = False):
    """[(name, amount|None, is_control)] with wrapped names rejoined per column.
    `grouped`: amounts must be thousands-grouped (the supplier schedule)."""
    pending: dict[int, str] = {}
    res = []
    tail = AMT_TAIL_GROUPED if grouped else AMT_TAIL
    for ci, text in cells(lines, fallback):
        tr = TRUNCATED_TAIL.match(text)
        if tr and not tail.match(text):
            name = tr.group(1).strip()
            if ci in pending:
                name = f"{pending.pop(ci)} {name}"
            res.append((name, None, False, tr.group(2)))
            continue
        m = tail.match(text)
        if not m or not re.search(r"[A-Za-z]", m.group(1)):
            lone = re.fullmatch(rf"\s*({AMOUNT})\s*", text)
            if lone and "$" in text:
                res.append(("Total", amount(lone.group(1)), True, None))
            elif not re.fullmatch(AMOUNT, text):
                pending[ci] = (pending.get(ci, "") + " " + text).strip()
            continue
        name = m.group(1).strip()
        if ci in pending and not CONTROL.search(name):
            name = f"{pending.pop(ci)} {name}"
        pending.pop(ci, None)
        name = re.sub(r"\s+\$$", "", re.sub(r"\s+", " ", name)).strip()
        res.append((name, amount(m.group(2)), bool(CONTROL.search(name)), None))
    return res


def is_city_supplier_page(pg: str) -> bool:
    first = next((l for l in pg.splitlines() if l.strip()), "").upper()
    head = pg[:400].upper()
    # the City's own schedule — not its reconciliation note, and not the Public
    # Library Board's separate schedule some years print after it (read off the
    # page's own header line: a supplier named "... Library ..." is not a header)
    return ("SUPPLIERS OF GOODS" in first or "PAYMENTS TO SUPPLIERS" in first or "AMENDMENT" in first) \
        and "SUPPLIERS OF GOODS" in head and "RECONCILIATION" not in head and "LIBRARY" not in first


def schedule_split(pgs: list[str]) -> int | None:
    from collections import Counter
    votes = Counter()
    for pg in pgs:
        if is_city_supplier_page(pg):
            c = column_split(pg.splitlines())
            if c is not None:
                votes[c] += 1
    return votes.most_common(1)[0][0] if votes else None


def parse_supplier_pages(pgs: list[str], year: int):
    rows, controls, unread = [], [], []
    fallback = schedule_split(pgs)
    for pno, pg in enumerate(pgs, start=1):
        head = pg[:400].upper()
        if not is_city_supplier_page(pg):
            continue
        lines = pg.splitlines()
        if "AMENDMENT" in head:
            # "Add:" rows until "Total amendment": name, over-$25k amount, total
            adding = False
            for s in lines:
                t = s.strip()
                if t.lower().startswith("add:"):
                    adding = True
                    continue
                m = re.match(rf"^(.*?\S)\s{{2,}}({AMOUNT})(?:\s{{2,}}({AMOUNT}))?(?:\s{{2,}}({AMOUNT}))?\s*$", t)
                if not m:
                    continue
                label = re.sub(r"\s+\$$", "", m.group(1)).strip()
                if re.match(r"^(revised total|previously reported total|total amendment)", label, re.I):
                    controls.append({"fiscal_year": year, "schedule": "suppliers", "label": label, "amount_cad": amount(m.group(2)), "page": pno})
                    adding = not label.lower().startswith("total amendment") and adding
                    continue
                if adding and not label.lower().startswith("under"):
                    rows.append({"fiscal_year": year, "name": label, "amount_cad": amount(m.group(2)),
                                 "is_individual": is_individual(label), "page": pno, "amendment": True})
            continue
        for name, amt, is_ctrl, amt_text in read_pairs(lines, fallback, grouped=True):
            if is_ctrl:
                controls.append({"fiscal_year": year, "schedule": "suppliers", "label": name, "amount_cad": amt, "page": pno})
            else:
                rows.append({"fiscal_year": year, "name": name, "amount_cad": amt, "amount_text": amt_text,
                             "is_individual": is_individual(name), "page": pno, "amendment": False})
                if amt is None:
                    unread.append((year, pno, name, amt_text))
    return rows, controls, unread


def parse_grant_pages(pgs: list[str], year: int):
    rows, controls = [], []
    for pno, pg in enumerate(pgs, start=1):
        head = pg[:400]
        m = re.search(r"GRANTS\s*[–\-]\s*([A-Za-z][A-Za-z &/,]+?)(?:\s+continued)?\s{2,}", head)
        if not m:
            continue
        category = re.sub(r"\s+continued$", "", m.group(1).strip(), flags=re.I).title()
        category = {"Community Services": "Community", "Business Improvement Association": "Business Improvement Associations"}.get(category, category)
        for name, amt, is_ctrl, amt_text in read_pairs(pg.splitlines()):
            if is_ctrl:
                controls.append({"fiscal_year": year, "schedule": f"grants:{category}", "label": name, "amount_cad": amt, "page": pno})
            else:
                rows.append({"fiscal_year": year, "category": category, "name": name, "amount_cad": amt, "amount_text": amt_text,
                             "is_individual": is_individual(name), "page": pno})
    return rows, controls


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2011-2025")
    ap.add_argument("--load", action="store_true")
    args = ap.parse_args()
    a, b = (int(x) for x in args.years.split("-")) if "-" in args.years else (int(args.years),) * 2
    all_s, all_g, all_c = [], [], []
    for year in range(a, b + 1):
        pdf = CACHE / f"{year}-statement.pdf"
        if not pdf.exists():
            print(f"{year}: missing {pdf.name}")
            continue
        pgs = repair(year, pages(pdf))
        s_rows, s_ctrl, unread = parse_supplier_pages(pgs, year)
        g_rows, g_ctrl = parse_grant_pages(pgs, year)
        s_sum = sum(r["amount_cad"] or 0 for r in s_rows)
        over = [c for c in s_ctrl if re.search(r">", c["label"])]
        oc = over_ctrl[0]["amount_cad"] if (over_ctrl := [c for c in s_ctrl if re.search(r"revised total", c["label"], re.I)] or [c for c in s_ctrl if re.search(r"(>|over)\s*\$?\s*25", c["label"], re.I)]) else None
        print(f"{year}: suppliers {len(s_rows):5d} rows Σ {s_sum:>15,}  over-25k control {oc}  delta {None if oc is None else oc - s_sum}  indiv {sum(r['is_individual'] for r in s_rows)}")
        over_ctrl = [c for c in s_ctrl if re.search(r"revised total", c["label"], re.I)] or \
                    [c for c in s_ctrl if re.search(r"(>|over)\s*\$?\s*25", c["label"], re.I)]
        for c in s_ctrl:
            c["parsed_sum_cad"] = s_sum
            c["delta_cad"] = (c["amount_cad"] - s_sum) if over_ctrl and c is over_ctrl[0] else None
        by_cat: dict[str, int] = {}
        for r in g_rows:
            by_cat[r["category"]] = by_cat.get(r["category"], 0) + (r["amount_cad"] or 0)
        for c in g_ctrl:
            cat = c["schedule"].split(":", 1)[1]
            c["parsed_sum_cad"] = by_cat.get(cat, 0)
            c["delta_cad"] = c["amount_cad"] - by_cat.get(cat, 0)
        print(f"       grants {len(g_rows):4d} rows  " + "  ".join(f"{k}: Δ{[c['amount_cad'] - v for c in g_ctrl if c['schedule'] == 'grants:' + k]}" for k, v in by_cat.items()))
        all_s += s_rows; all_g += g_rows; all_c += s_ctrl + g_ctrl
        for u in unread[:3]:
            print("   unread", u)
    if args.load:
        import pandas as pd
        from google.cloud import bigquery
        from common import PROJECT_ID, get_bigquery_client, RAW_DATASET, CORE_DATASET, MARTS_DATASET  # noqa: E402
        c = get_bigquery_client()
        now = datetime.now(timezone.utc)
        for name, rows in (("ca_vancouver_sofi_suppliers", all_s), ("ca_vancouver_sofi_grants", all_g), ("ca_vancouver_sofi_controls", all_c)):
            df = pd.DataFrame(rows)
            df["amount_cad"] = df["amount_cad"].astype("Int64")   # an unread amount stays NULL, not 0.0
            df["_synced_at"] = now
            c.load_table_from_dataframe(df, f"{PROJECT_ID}.{RAW_DATASET}.{name}",
                                        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
            print(f"loaded {len(df)} → raw.{name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
