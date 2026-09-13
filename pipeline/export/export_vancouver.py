#!/usr/bin/env python3
"""
Export the Vancouver marts from BigQuery to JSON files in $VAN_OUTPUT_DIR
(default outputs/).

Outputs:
    - index.json            : file manifest + shared provenance.
    - landing.json          : the hub page payload — headline figures for
      contracts, council votes, payroll (aggregate), the parcel fabric and
      population, EACH beside its source_url / licence / as_of.
      Source: mart_ca_vancouver_landing.
    - contracts_top.json    : the 25 largest awarded contracts.
      Source: mart_ca_vancouver_top_contracts.
    - votes_contested.json  : the 25 most-opposed council items (tallies).
      Source: mart_ca_vancouver_contested_votes.
    - contracts.json        : the contracts page — headline, awards by year
      (with procedure mix), top vendors, and a slim row per competition for
      the explorer. Sources: mart_ca_vancouver_{landing,contracts_years,
      vendors,bids}.
    - bids.json             : one record per competition (bid_number) with
      every response the City published — winners AND losing bids with
      their price. Sources: mart_ca_vancouver_{bids,contracts}.
    - vendors.json          : one record per vendor (spelling variants
      merged on vendor_key) — competitions entered/won, awarded total by
      year. Sources: mart_ca_vancouver_{vendors,contracts}.

    - budget.json           : revenue by source, expense by function and by
      object, per resident, 2015→ (SOFI five-year review, each table-year
      from the newest print; misprints listed). Sources:
      mart_ca_vancouver_finance_{years,lines,sources}.
    - debt.json             : debenture debt, sinking fund, per capita,
      reserves by group, balance sheet. Same sources.
    - payments.json         : SOFI suppliers over $25,000 and grants by year
      and category, printed totals beside parsed ones, the payee list for the
      explorer. Sources: mart_ca_vancouver_{payments_years,payees}.
    - payees.json           : one record per named payee (organisations and
      public bodies; withheld names never exported).
    - payroll.json          : employees over $75,000 — years, pay bands,
      departments with titles held by ≥3 people. Aggregates only.

Data contract: every file carries generated_at + source_pipeline;
every block carries source/source_url/license/as_of read off the mart's own
columns (stamped from the ODS catalog snapshot) — nothing hardcoded.

Privacy: SOFI remuneration is exported as year × count × total only.
No person name leaves the warehouse.

Plumbing lives in pipeline/common.py.

Usage:
    python pipeline/export/export_vancouver.py
Prereq: cd dbt && dbt build
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # pipeline/
from common import MARTS_DATASET, OUTPUT_DIR, PROJECT_ID, Logger, get_bigquery_client, write_json  # noqa: E402

CITY = "ca/vancouver"
FAMILY = "ca"
SOURCE_PIPELINE = (
    "config/vancouver.yaml → pipeline/sync → raw → dbt staging → analytics → marts → pipeline/export/export_vancouver.py"
)


def _j(v):
    """JSON-safe, recursive: Decimal → float, date/datetime → ISO string,
    STRUCT/ARRAY columns walked."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dict):
        return {k: _j(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_j(x) for x in v]
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def rows(client, table: str, extra: str = "") -> list[dict]:
    q = f"SELECT * FROM `{PROJECT_ID}.{MARTS_DATASET}.{table}` {extra}"
    return [{k: _j(v) for k, v in dict(r).items()} for r in client.query(q).result()]


def envelope() -> dict:
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "source_pipeline": SOURCE_PIPELINE}


def block(r: dict, prefix: str, name_key: str | None = None) -> dict:
    """Provenance sub-object read off the mart row's own columns."""
    name_key = name_key or f"{prefix}_source_name"
    return {
        "name": r.get(name_key),
        "source_url": r.get(f"{prefix}_source_url"),
        "license": r.get(f"{prefix}_license"),
        "as_of": r.get(f"{prefix}_as_of"),
    }


def city_outline(client) -> dict | None:
    """The city's outline for the hub hero: the 22 local areas unioned and
    simplified in BigQuery, projected into a 200-wide viewBox (north up)."""
    import json as _json
    import math
    q = f"""SELECT ST_ASGEOJSON(ST_SIMPLIFY(ST_UNION_AGG(geog), 60)) AS g
            FROM `{PROJECT_ID}.{MARTS_DATASET}.mart_ca_vancouver_city_local_areas`"""
    try:
        (row,) = list(client.query(q).result())
    except Exception:
        return None
    geo = _json.loads(row.g)
    polys = geo["coordinates"] if geo["type"] == "MultiPolygon" else [geo["coordinates"]]
    pts = [pt for poly in polys for ring in poly[:1] for pt in ring]
    lon0 = min(p[0] for p in pts); lon1 = max(p[0] for p in pts)
    lat0 = min(p[1] for p in pts); lat1 = max(p[1] for p in pts)
    k = math.cos(math.radians((lat0 + lat1) / 2))
    w = (lon1 - lon0) * k; h = lat1 - lat0
    sc = 200 / w
    paths = []
    for poly in polys:
        ring = poly[0]
        d = " ".join(("M" if i == 0 else "L") + f"{(x - lon0) * k * sc:.1f} {(lat1 - y) * sc:.1f}" for i, (x, y) in enumerate(ring)) + " Z"
        paths.append(d)
    return {"viewBox": f"0 0 200 {h * sc:.0f}", "paths": paths}


def build_landing(client, log) -> dict:
    (r,) = rows(client, "mart_ca_vancouver_landing")
    payload = {
        **envelope(),
        "unit": r["unit"],
        "outline": city_outline(client),
        "population": {
            "value": r["population"], "census_year": r["census_year"],
            "source": {"name": r["pop_source"], "source_url": r["pop_source_url"]},
        },
        "contracts": {
            "n": r["n_contracts"], "total_cad": r["contracts_total_cad"],
            "n_competitions": r["n_competitions"], "n_responses": r["n_responses"],
            "first_year": r["contracts_first_year"], "last_year": r["contracts_last_year"],
            "n_vendors": r["n_vendors"], "n_bidders": r["n_bidders"],
            "latest_complete_year": {"year": r["contracts_latest_year"], "total_cad": r["contracts_latest_total_cad"], "n": r["contracts_latest_n"]},
            "source": block(r, "contracts"),
            "note": "Tender side — what the City posted and awarded. The register lists every response to a competition, winners and losing bids alike; n counts the awards. Payments to suppliers exist only in the annual Statement of Financial Information (PDF).",
        },
        "votes": {
            "n_votes_cast": r["n_votes_cast"], "n_items": r["n_items"],
            "first_date": r["votes_first_date"], "last_date": r["votes_last_date"],
            "n_opposition": r["n_opposition"], "n_conflicts": r["n_conflicts"], "n_rezoning_items": r["n_rezoning_items"],
            "source": block(r, "votes"),
        },
        "payroll": {
            "first_year": r["pay_first_year"], "latest_year": r["pay_latest_year"],
            "latest_n_people": r["pay_latest_n_people"], "latest_total_cad": r["pay_latest_total_cad"],
            "source": block(r, "pay"),
            "note": "Employees earning over $75,000, as the City publishes from its Statement of Financial Information. Aggregates only.",
        },
        "fabric": {
            "n_parcels": r["n_parcels"], "n_polygons": r["n_polygons"], "n_polygons_unkeyed": r["n_polygons_unkeyed"],
            "n_parcels_with_roll": r["n_parcels_with_roll"],
            "n_properties": r["n_properties"], "n_parcels_dated": r["n_parcels_dated"],
            "oldest_year_built": r["oldest_year_built"], "roll_year": r["roll_year"],
            "land_value_total_cad": r["land_value_total_cad"], "tax_levy_total_cad": r["tax_levy_total_cad"],
            "sources": {
                "parcels": {"name": r["parcels_source_name"], "source_url": r["parcels_source_url"]},
                "roll": block(r, "roll"),
            },
            "note": "year_built describes buildings standing in the 2025 roll (survivorship). A parcel with several strata units is one parcel, many properties.",
        },
    }
    log.data("contracts", r["n_contracts"]); log.data("votes cast", r["n_votes_cast"])
    log.data("parcels", r["n_parcels"]); log.data("population", r["population"])
    return payload


def build_top_contracts(client, log) -> dict:
    items = rows(client, "mart_ca_vancouver_top_contracts", "ORDER BY bid_amount_cad DESC")
    src = items[0] if items else {}
    log.data("top contracts", len(items))
    return {
        **envelope(), "unit": "CAD",
        "source": {"name": src.get("source_name"), "source_url": src.get("source_url"), "license": src.get("source_license"), "as_of": src.get("source_rows_updated_at")},
        "items": [{k: v for k, v in it.items() if not k.startswith("source_") and k != "unit"} for it in items],
    }


def build_contested(client, log) -> dict:
    items = rows(client, "mart_ca_vancouver_contested_votes", "ORDER BY n_opposition DESC, vote_date DESC")
    src = items[0] if items else {}
    log.data("contested items", len(items))
    return {
        **envelope(),
        "source": {"name": src.get("source_name"), "source_url": src.get("source_url"), "license": src.get("source_license"), "as_of": src.get("source_rows_updated_at")},
        "items": [{k: v for k, v in it.items() if not k.startswith("source_")} for it in items],
    }


def _src(sample: dict) -> dict:
    return {"name": sample.get("source_name"), "source_url": sample.get("source_url"),
            "license": sample.get("source_license"), "as_of": sample.get("source_rows_updated_at")}


def _strip(d: dict, drop=("unit",)) -> dict:
    return {k: v for k, v in d.items() if not k.startswith("source_") and k not in drop}


def _load_contract_marts(client):
    responses = rows(client, "mart_ca_vancouver_contracts")
    bids = rows(client, "mart_ca_vancouver_bids")
    vendors = rows(client, "mart_ca_vancouver_vendors")
    years = rows(client, "mart_ca_vancouver_contracts_years", "ORDER BY award_year")
    if not (responses and bids and vendors and years):
        raise RuntimeError("a Vancouver contracts mart is empty — run dbt build")
    for v in vendors:
        if not v["vendor_key"]:
            raise RuntimeError(f"empty vendor_key for {v['vendor_name']!r} — the slug rule in core_ca_vancouver_contracts drops it")
    return responses, bids, vendors, years


def build_contracts_pages(client, log) -> tuple[dict, dict, dict]:
    """contracts.json + bids.json + vendors.json from one read of the marts,
    so the three files can never disagree with each other."""
    responses, bids, vendors, years = _load_contract_marts(client)
    (land,) = rows(client, "mart_ca_vancouver_landing")
    src = _src(bids[0])
    vendor_name = {v["vendor_key"]: v["vendor_name"] for v in vendors}

    by_bid: dict[str, list[dict]] = {}
    for r in responses:
        by_bid.setdefault(r["bid_number"], []).append(r)

    # ── bids.json ────────────────────────────────────────────────────────────
    bid_items: dict[str, dict] = {}
    for b in bids:
        rs = by_bid.get(b["bid_number"], [])
        # Winners first, then by price ascending (unpriced last) — the order a
        # reader compares offers in.
        rs_sorted = sorted(rs, key=lambda x: (not x["is_awarded"], x["bid_amount_cad"] is None, x["bid_amount_cad"] or 0, x["vendor_name"]))
        bid_items[b["bid_number"]] = {
            **_strip(b),
            "responses": [{
                "vendor_name": x["vendor_name"], "vendor_key": x["vendor_key"],
                "amount_cad": x["bid_amount_cad"], "is_awarded": x["is_awarded"], "awarded": x["awarded"],
                "award_date": x["award_date"],
                # one amount printed beside several winners: counted once in totals
                "shared_with": (x["n_sharing_winners"] - 1) if x["is_shared_amount"] else 0,
                # Lot-level descriptions ("Weeks House") only when they differ
                # from the competition's own.
                "lot": x["bid_description"] if x["bid_description"] and x["bid_description"] != b["bid_description"] else None,
            } for x in rs_sorted],
        }

    # ── vendors.json ─────────────────────────────────────────────────────────
    by_vendor: dict[str, list[dict]] = {}
    for r in responses:
        by_vendor.setdefault(r["vendor_key"], []).append(r)
    bid_meta = {b["bid_number"]: b for b in bids}
    vendor_items: dict[str, dict] = {}
    for v in vendors:
        rs = by_vendor.get(v["vendor_key"], [])
        comps: dict[str, dict] = {}
        for x in rs:
            c = comps.setdefault(x["bid_number"], {"bid_number": x["bid_number"], "won": False, "n_responses": 0})
            c["n_responses"] += 1
            c["won"] = c["won"] or x["is_awarded"]
        # Amount shown per competition = what this vendor was AWARDED when it
        # won; its lowest offer when it did not.
        for num, c in comps.items():
            mine = [x for x in rs if x["bid_number"] == num]
            won = [x for x in mine if x["is_awarded"]]
            priced_won = [x["bid_amount_cad"] for x in won if x["bid_amount_cad"] is not None]
            priced_all = [x["bid_amount_cad"] for x in mine if x["bid_amount_cad"] is not None]
            c["amount_cad"] = sum(priced_won) if won and priced_won else (None if won else (min(priced_all) if priced_all else None))
            m = bid_meta[num]
            c.update({"bid_description": m["bid_description"], "bid_type_group": m["bid_type_group"],
                      "award_year": m["award_year"], "n_bidders": m["n_bidders"]})
        comp_list = sorted(comps.values(), key=lambda c: (-(c["award_year"] or 0), not c["won"], -(c["amount_cad"] or 0)))
        yr: dict[int, dict] = {}
        for x in rs:
            if x["award_year"] is None:
                continue
            y = yr.setdefault(x["award_year"], {"year": x["award_year"], "awarded_cad": 0.0, "n_won": 0, "n_entered": set()})
            y["n_entered"].add(x["bid_number"])
            if x["is_awarded"]:
                y["n_won"] += 1
                y["awarded_cad"] += x["bid_amount_cad"] or 0
        vendor_items[v["vendor_key"]] = {
            **_strip(v),
            "competitions": comp_list,
            "by_year": [{"year": y["year"], "awarded_cad": round(y["awarded_cad"], 2), "n_won": y["n_won"], "n_entered": len(y["n_entered"])}
                        for y in sorted(yr.values(), key=lambda y: y["year"])],
        }

    # ── contracts.json ───────────────────────────────────────────────────────
    # Comparable-from year: the register's first years show mostly a single
    # response per competition; the first year where single-response
    # competitions fall under half is where losing bids appear routinely.
    comparable_from = next((y["award_year"] for y in years if y["n_competitions"] and y["n_single_bidder"] / y["n_competitions"] < 0.5), None)
    type_tot: dict[str, dict] = {}
    for y in years:
        for g in y["by_type"] or []:
            t = type_tot.setdefault(g["bid_type_group"], {"bid_type_group": g["bid_type_group"], "n_competitions": 0, "awarded_total_cad": 0.0})
            t["n_competitions"] += g["n"]
            t["awarded_total_cad"] += g["awarded_total_cad"] or 0
    top_vendors = sorted((v for v in vendors if v["awarded_total_cad"]), key=lambda v: -v["awarded_total_cad"])[:15]

    def slim(b: dict) -> dict:
        winners = [x for x in by_bid.get(b["bid_number"], []) if x["is_awarded"]]
        wkeys = sorted({x["vendor_key"] for x in winners})
        return {
            "id": b["bid_number"], "d": b["bid_description"], "t": b["bid_type_group"], "y": b["award_year"],
            "nb": b["n_bidders"], "na": b["n_awarded"], "amt": b["awarded_total_cad"],
            "w": vendor_name[wkeys[0]] if len(wkeys) == 1 else None, "nw": len(wkeys),
        }

    comps_slim = sorted((slim(b) for b in bids), key=lambda c: (-(c["amt"] or 0), -(c["y"] or 0)))
    contracts = {
        **envelope(), "unit": "CAD", "source": src,
        "headline": {
            "n_awards": land["n_contracts"], "awarded_total_cad": land["contracts_total_cad"],
            "n_awards_unpriced": sum(b["n_awarded_unpriced"] for b in bids),
            "n_competitions": land["n_competitions"], "n_responses": land["n_responses"],
            "n_competitions_unawarded": sum(1 for b in bids if b["n_awarded"] == 0),
            "n_vendors_awarded": land["n_vendors"], "n_bidders": land["n_bidders"],
            "first_year": land["contracts_first_year"], "last_year": land["contracts_last_year"],
            "comparable_from": comparable_from,
        },
        "years": [_strip(y) for y in years],
        "types": sorted(type_tot.values(), key=lambda t: -t["awarded_total_cad"]),
        "top_vendors": [{"vendor_key": v["vendor_key"], "vendor_name": v["vendor_name"], "awarded_total_cad": v["awarded_total_cad"],
                         "n_competitions": v["n_competitions"], "n_competitions_won": v["n_competitions_won"]} for v in top_vendors],
        "competitions": comps_slim,
        "notes": {
            "grain": "One row per response to a posted competition. A competition can award several vendors (pre-qualified pools, lots).",
            "unpriced": "Some awards are published without an amount (pre-qualifications, standing offers); they count as awards but add nothing to totals.",
            "tender_side": "Awarded amounts are what the City agreed at award. What it later paid each supplier is published only in the annual Statement of Financial Information.",
        },
    }
    log.data("competitions", len(bid_items)); log.data("vendors", len(vendor_items)); log.data("comparable_from", comparable_from)
    return (contracts,
            {**envelope(), "unit": "CAD", "source": src, "items": bid_items},
            {**envelope(), "unit": "CAD", "source": src, "items": vendor_items})


# ── finance: SOFI five-year review + payment schedules + remuneration ──────
def _sofi_docs(client) -> tuple[list[dict], dict[int, dict]]:
    """Every SOFI document, and per fiscal year the one its schedules were
    read from (the supply-management file in 2017/2018)."""
    docs = rows(client, "mart_ca_vancouver_finance_sources", "ORDER BY fiscal_year, part")
    if not docs:
        raise RuntimeError("mart_ca_vancouver_finance_sources is empty — run dbt build")
    sched: dict[int, dict] = {}
    for d in docs:
        if any(u in ("suppliers", "grants") for u in d["used_for"]):
            sched[d["fiscal_year"]] = d
    return docs, sched


def _doc_ref(d: dict | None, page: int | None = None) -> dict | None:
    if not d:
        return None
    return {"fiscal_year": d["fiscal_year"], "part": d["part"], "url": d["wayback_url"],
            "original_url": d["original_url"], "captured_on": d["captured_on"], "page": page}


def _series(lines: list[dict], table: str, section: str | None) -> dict:
    """One chart's lines: [{key, label, order, values{year: $}, shares{year: 0..1}}],
    the printed totals by year, and comparable_from."""
    sel = [l for l in lines if l["table_name"] == table and (section is None or l["section_key"] == section)]
    out: dict[str, dict] = {}
    totals: dict[str, float] = {}
    comparable_from = None
    for l in sel:
        y = str(l["fiscal_year"])
        comparable_from = l["comparable_from"] if l["comparable_from"] is not None else comparable_from
        if l["line_key"] == "total":
            totals[y] = l["amount_cad"] if l["amount_cad"] is not None else l["value_as_printed"]
            continue
        if l["is_summary"]:
            continue
        o = out.setdefault(l["line_key"], {"key": l["line_key"], "label": l["label"], "order": l["line_order"], "values": {}, "shares": {}})
        o["values"][y] = l["amount_cad"] if l["amount_cad"] is not None else l["value_as_printed"]
        if l["share_of_section"] is not None:
            o["shares"][y] = round(l["share_of_section"], 5)
    return {"comparable_from": comparable_from, "totals": totals,
            "lines": sorted(out.values(), key=lambda x: (x["order"] if x["order"] is not None else 999, x["key"]))}


def build_finance_pages(client, log) -> tuple[dict, dict]:
    years = rows(client, "mart_ca_vancouver_finance_years", "ORDER BY fiscal_year")
    lines = rows(client, "mart_ca_vancouver_finance_lines", "ORDER BY table_name, section_key, line_order, fiscal_year")
    docs, _ = _sofi_docs(client)
    if not (years and lines):
        raise RuntimeError("a Vancouver finance mart is empty — run dbt build")
    by_url = {d["wayback_url"]: d for d in docs}
    last = years[-1]
    first = years[0]

    def ops_ref(y: dict) -> dict | None:
        return _doc_ref(by_url.get(y["operations_source_url"]), y["operations_page"])

    budget = {
        **envelope(), "unit": "CAD",
        "headline": {
            "year": last["fiscal_year"], "first_year": first["fiscal_year"],
            "revenues_cad": last["revenues_cad"], "expenses_cad": last["expenses_cad"], "surplus_cad": last["surplus_cad"],
            "population": last["population"], "expenses_per_resident_cad": last["expenses_per_resident_cad"],
            "expenses_per_resident_month_cad": last["expenses_per_resident_cad"] / 12 if last["expenses_per_resident_cad"] else None,
            "property_tax_cad": last["property_tax_cad"], "capital_additions_cad": last["capital_additions_cad"],
            "wages_cad": last["wages_cad"],
            "source": ops_ref(last),
        },
        "years": [{
            "year": y["fiscal_year"], "revenues_cad": y["revenues_cad"], "expenses_cad": y["expenses_cad"],
            "surplus_cad": y["surplus_cad"], "population": y["population"],
            "expenses_per_resident_cad": y["expenses_per_resident_cad"],
            "expenses_per_resident_month_cad": y["expenses_per_resident_cad"] / 12 if y["expenses_per_resident_cad"] else None,
            "capital_additions_cad": y["capital_additions_cad"],
            "property_tax_cad": y["property_tax_cad"], "developer_contributions_cad": y["developer_contributions_cad"],
            "source": ops_ref(y),
        } for y in years],
        "revenues": _series(lines, "operations", "revenues"),
        "expenses": _series(lines, "operations", "expenses"),
        "object": _series(lines, "expenses_by_object", "expenses"),
        "tax_class": _series(lines, "taxation", "propertytaxrevenuebypropertyclass"),
        "documents": [_doc_ref(d) | {"used_for": d["used_for"], "misprints": d["misprints"]} for d in docs if d["part"] == "statement" and d["used_for"]],
    }
    misprints = [m | {"report_year": d["fiscal_year"], "url": d["wayback_url"]} for d in docs for m in d["misprints"]]
    budget["misprints"] = misprints

    def debt_ref(y: dict) -> dict | None:
        return _doc_ref(by_url.get(y["debt_source_url"]), y["debt_page"])

    reserves_groups = []
    for sec in sorted({l["section_key"] for l in lines if l["table_name"] == "reserves" and l["section_key"] not in (None, "all")}):
        ser = _series(lines, "reserves", sec)
        label = next((l["section"] for l in lines if l["table_name"] == "reserves" and l["section_key"] == sec and l["fiscal_year"] == last["fiscal_year"]), sec)
        if sec == "futuredebtrepayment":
            ser["totals"] = {k: v for ln in ser["lines"] for k, v in ln["values"].items()}
        reserves_groups.append({"key": sec, "label": label, **ser})
    debt = {
        **envelope(), "unit": "CAD",
        "headline": {
            "year": last["fiscal_year"], "first_year": first["fiscal_year"],
            "debenture_debt_cad": last["debenture_debt_cad"], "external_debt_cad": last["external_debt_cad"], "internal_debt_cad": last["internal_debt_cad"],
            "sinking_fund_cad": last["sinking_fund_cad"], "net_external_debt_cad": last["net_external_debt_cad"],
            "gross_debt_per_capita_cad": last["gross_debt_per_capita_cad"], "net_debt_per_capita_cad": last["net_debt_per_capita_cad"],
            "population": last["population"], "reserves_cad": last["reserves_cad"],
            "accumulated_surplus_cad": last["accumulated_surplus_cad"], "tangible_capital_assets_cad": last["tangible_capital_assets_cad"],
            "net_financial_assets_cad": last["net_financial_assets_cad"], "source": debt_ref(last),
        },
        "years": [{
            "year": y["fiscal_year"], "debenture_debt_cad": y["debenture_debt_cad"], "external_debt_cad": y["external_debt_cad"],
            "sinking_fund_cad": y["sinking_fund_cad"], "net_external_debt_cad": y["net_external_debt_cad"],
            "gross_debt_per_capita_cad": y["gross_debt_per_capita_cad"], "net_debt_per_capita_cad": y["net_debt_per_capita_cad"],
            "population": y["population"], "reserves_cad": y["reserves_cad"], "accumulated_surplus_cad": y["accumulated_surplus_cad"],
            "tangible_capital_assets_cad": y["tangible_capital_assets_cad"], "net_financial_assets_cad": y["net_financial_assets_cad"],
            "source": debt_ref(y),
        } for y in years],
        "reserves": reserves_groups,
        "balance": {k: _series(lines, "financial_position", k) for k in ("financialassets", "liabilities", "nonfinancialassets")},
        "documents": budget["documents"],
    }
    log.data("finance years", len(years)); log.data("finance lines", len(lines))
    return budget, debt


def build_payments_pages(client, log) -> tuple[dict, dict]:
    yrs = rows(client, "mart_ca_vancouver_payments_years", "ORDER BY fiscal_year, schedule, category")
    payees = rows(client, "mart_ca_vancouver_payees", "ORDER BY total_cad DESC, payee_key")
    docs, sched = _sofi_docs(client)
    if not (yrs and payees):
        raise RuntimeError("a Vancouver payments mart is empty — run dbt build")

    by_year: dict[int, dict] = {}
    for r in yrs:
        y = by_year.setdefault(r["fiscal_year"], {"year": r["fiscal_year"], "suppliers": None, "grants": {"total_cad": 0.0, "withheld_cad": 0.0, "n_lines": 0, "categories": []},
                                                  "source": _doc_ref(sched.get(r["fiscal_year"]))})
        block_ = {"total_cad": r["total_cad"], "printed_total_cad": r["printed_total_cad"], "delta_cad": r["delta_cad"],
                  "withheld_cad": r["withheld_cad"], "n_withheld_lines": r["n_withheld_lines"],
                  "public_bodies_cad": r["public_bodies_cad"], "organisations_cad": r["organisations_cad"], "n_lines": r["n_lines"], "n_payees_named": r["n_payees_named"],
                  "status": r["status"], "gap_reason": r["gap_reason"]}
        if r["schedule"] == "suppliers":
            y["suppliers"] = block_ | {"under_threshold_cad": r["under_threshold_cad"]}
        else:
            g = y["grants"]
            g["total_cad"] += r["total_cad"]; g["withheld_cad"] += r["withheld_cad"]; g["n_lines"] += r["n_lines"]
            g["categories"].append(block_ | {"category": r["category"]})
    for y in by_year.values():
        y["grants"]["categories"].sort(key=lambda c: -c["total_cad"])
    years = [by_year[k] for k in sorted(by_year)]
    last = years[-1]

    def slim(p: dict) -> dict:
        return {"k": p["payee_key"], "n": p["payee_name"], "t": p["total_cad"], "s": p["supplier_total_cad"], "g": p["grant_total_cad"],
                "y0": p["first_year"], "y1": p["last_year"], "pb": p["is_public_body"]}

    latest = sorted((p for p in payees if any(b["fiscal_year"] == last["year"] for b in p["by_year"])),
                    key=lambda p: -sum(b["supplier_cad"] + b["grant_cad"] for b in p["by_year"] if b["fiscal_year"] == last["year"]))
    grant_cats: dict[str, dict] = {}
    for y in years:
        for c in y["grants"]["categories"]:
            gc = grant_cats.setdefault(c["category"], {"category": c["category"], "values": {}})
            gc["values"][str(y["year"])] = c["total_cad"]

    payments = {
        **envelope(), "unit": "CAD",
        "headline": {
            "year": last["year"], "first_year": years[0]["year"],
            "suppliers_total_cad": last["suppliers"]["total_cad"], "suppliers_printed_cad": last["suppliers"]["printed_total_cad"],
            "under_threshold_cad": last["suppliers"]["under_threshold_cad"], "public_bodies_cad": last["suppliers"]["public_bodies_cad"],
            "grants_total_cad": last["grants"]["total_cad"], "n_payees_named_all_years": len(payees),
            "suppliers_total_all_years_cad": sum(y["suppliers"]["total_cad"] for y in years),
            "grants_total_all_years_cad": sum(y["grants"]["total_cad"] for y in years),
            "withheld_all_years_cad": sum(y["suppliers"]["withheld_cad"] + y["grants"]["withheld_cad"] for y in years),
            # share of the latest year's payments (suppliers over $25k + grants,
            # withheld names included in the denominator) that went to its 10
            # largest named payees
            "top10_latest_share": (sum(sum(b["supplier_cad"] + b["grant_cad"] for b in p_["by_year"] if b["fiscal_year"] == last["year"]) for p_ in latest[:10])
                                   / (last["suppliers"]["total_cad"] + last["grants"]["total_cad"])) if latest else None,
            "source": last["source"],
        },
        "years": years,
        "grant_categories": sorted(grant_cats.values(), key=lambda c: -sum(c["values"].values())),
        "top_organisations": [slim(p) for p in payees if not p["is_public_body"]][:25],
        "top_public_bodies": [slim(p) for p in payees if p["is_public_body"]][:12],
        "top_latest": [slim(p) | {"ty": sum(b["supplier_cad"] + b["grant_cad"] for b in p["by_year"] if b["fiscal_year"] == last["year"])} for p in latest[:25]],
        "top_grants": [slim(p) for p in sorted(payees, key=lambda p: -p["grant_total_cad"]) if p["grant_total_cad"] > 0][:25],
        "payees": [slim(p) for p in payees],
        "rules": {
            "public_body": "Other governments, their agencies and Crown corporations, and the statutory remittances the schedule lists beside suppliers (pension plan, workers' compensation, Receiver General) — matched on the printed name.",
            "withheld": "A name is published only when it carries an organisation marker (a legal form, a kind of body, a trade word). A name without one could be a person's: its amount is counted in every total, its name is not shown.",
            "payee_key": "Spellings of one organisation across years are merged when they are the same words apart from legal form and the schedules' abbreviations (\"Vanc.\", \"Prov.\").",
        },
        "documents": [_doc_ref(d) for d in docs if any(u in ("suppliers", "grants") for u in d["used_for"])],
    }
    # by_year as [year, supplier $, grant $] triples and one documents map for
    # the file: 6,953 payees × 15 years repeated as objects was 17 MB
    payee_items = {p["payee_key"]: {**p, "by_year": [[b["fiscal_year"], b["supplier_cad"], b["grant_cad"]] for b in p["by_year"]],
                                    "categories": [[c["category"], c["grant_cad"], c["n_years"]] for c in p["categories"]]}
                   for p in payees}
    payees_file = {**envelope(), "unit": "CAD",
                   "documents_by_year": {str(y): _doc_ref(d) for y, d in sorted(sched.items())},
                   "items": payee_items}
    log.data("payment years", len(years)); log.data("named payees", len(payees))
    return payments, payees_file


def build_payroll_page(client, log) -> dict:
    yrs = rows(client, "mart_ca_vancouver_payroll_years", "ORDER BY fiscal_year")
    bands = rows(client, "mart_ca_vancouver_payroll_bands", "ORDER BY fiscal_year, band_floor_cad")
    depts = rows(client, "mart_ca_vancouver_payroll_departments", "ORDER BY fiscal_year, remuneration_total_cad DESC")
    if not (yrs and bands and depts):
        raise RuntimeError("a Vancouver payroll mart is empty — run dbt build")
    last = yrs[-1]
    src = {"name": last["source_name"], "source_url": last["source_url"], "license": last["source_license"], "as_of": last["source_as_of"]}
    b_by: dict[str, list] = {}
    for b in bands:
        b_by.setdefault(str(b["fiscal_year"]), []).append({"floor_cad": b["band_floor_cad"], "n_people": b["n_people"], "total_cad": b["remuneration_total_cad"]})
    log.data("payroll years", len(yrs))
    return {
        **envelope(), "unit": "CAD", "source": src,
        "headline": {"year": last["fiscal_year"], "first_year": yrs[0]["fiscal_year"], "n_people": last["n_people"],
                     "total_cad": last["remuneration_total_cad"], "median_cad": last["median_cad"], "p90_cad": last["p90_cad"],
                     "n_departments": last["n_departments"]},
        "years": [_strip(y) for y in yrs],
        "bands": b_by,
        "departments": [_strip(d) for d in depts],
    }


# ── capital, places, housing ────────────────────────────────────────────────
def build_investments_page(client, log) -> dict:
    years = rows(client, "mart_ca_vancouver_capital_years", "ORDER BY budget_year")
    cats = rows(client, "mart_ca_vancouver_capital_categories", "ORDER BY budget_year, expenditure_budget_cad DESC")
    plan = rows(client, "mart_ca_vancouver_capital_plan", "ORDER BY service_category_1, revised_plan_cad DESC")
    progs = rows(client, "mart_ca_vancouver_capital_programs", "ORDER BY expenditure_budget_all_years_cad DESC NULLS LAST, program_key")
    if not (years and cats and plan and progs):
        raise RuntimeError("a Vancouver capital mart is empty — run dbt build")
    last = years[-1]
    plan_src = _src(plan[0])
    plan_by_cat: dict[str, dict] = {}
    for r in plan:
        c = plan_by_cat.setdefault(r["service_category_1"], {"category": r["service_category_1"], "revised_plan_cad": 0.0, "original_plan_cad": 0.0,
                                                           "city_led_cad": 0.0, "developer_in_kind_cad": 0.0, "remainder_cad": 0.0, "subcategories": []})
        for k in ("revised_plan_cad", "original_plan_cad", "city_led_cad", "developer_in_kind_cad", "remainder_cad"):
            c[k] += r[k] or 0
        c["subcategories"].append({"subcategory": r["service_category_2"], "n_programs": r["n_programs"], "revised_plan_cad": r["revised_plan_cad"]})
    plan_cats = sorted(plan_by_cat.values(), key=lambda c: -c["revised_plan_cad"])
    log.data("capital years", len(years)); log.data("capital programs", len(progs))
    return {
        **envelope(), "unit": "CAD",
        "headline": {
            "year": last["budget_year"], "first_year": years[0]["budget_year"],
            "expenditure_budget_cad": last["expenditure_budget_cad"], "new_multi_year_cad": last["new_multi_year_cad"],
            "n_programs": last["n_programs"],
            "plan_revised_cad": sum(c["revised_plan_cad"] for c in plan_cats),
            "plan_original_cad": sum(c["original_plan_cad"] for c in plan_cats),
            "plan_developer_in_kind_cad": sum(c["developer_in_kind_cad"] for c in plan_cats),
            "source": _src(last),
        },
        "years": [{
            "year": y["budget_year"], "capital_plan": y["capital_plan"], "n_programs": y["n_programs"],
            "expenditure_budget_cad": y["expenditure_budget_cad"], "new_multi_year_cad": y["new_multi_year_cad"],
            "funding": {k: y[f"new_{k}_cad"] for k in ("pay_as_you_go", "debt", "tax_fee_reserves", "development_reserves", "connections", "partner")},
            "source": _src(y),
        } for y in years],
        "categories": [{"year": c["budget_year"], "capital_plan": c["capital_plan"], "category": c["service_category_1"],
                        "expenditure_budget_cad": c["expenditure_budget_cad"], "new_multi_year_cad": c["new_multi_year_cad"],
                        "share": c["share_of_year"]} for c in cats],
        "plan": {"source": plan_src, "categories": plan_cats},
        # slim program rows for the explorer: key, name, category, first/last
        # year, expenditure budget each year [[year, $]]
        "programs": [{"k": p_["program_key"], "n": p_["program_name"], "c": p_["service_category_1"], "c2": p_["service_category_2"],
                      "y0": p_["first_year"], "y1": p_["last_year"], "e": p_["expenditure_budget_all_years_cad"],
                      "nb": p_["new_multi_year_all_years_cad"],
                      "ys": [[x["budget_year"], x["expenditure_budget_cad"]] for x in p_["years"]]} for p_ in progs],
    }


def build_places_pages(client, log) -> tuple[dict, dict]:
    places = rows(client, "mart_ca_vancouver_places", "ORDER BY place_kind, place_name")
    if not places:
        raise RuntimeError("mart_ca_vancouver_places is empty — run dbt build")
    import json as _json
    by_kind: dict[str, int] = {}
    for p_ in places:
        by_kind[p_["place_kind"]] = by_kind.get(p_["place_kind"], 0) + 1
    sources = {}
    for p_ in places:
        sources.setdefault(p_["place_kind"], {"name": p_["source_name"], "source_url": p_["source_url"], "license": p_["source_license"], "as_of": p_["source_rows_updated_at"]})
    index = {
        **envelope(), "unit": "CAD",
        "headline": {"n_places": len(places), "by_kind": by_kind,
                     "n_with_council": sum(1 for p_ in places if p_["n_council"]), "n_with_capital": sum(1 for p_ in places if p_["n_capital"]),
                     "n_permits": sum(p_["n_permits"] for p_ in places)},
        "sources": sources,
        "places": [{"id": p_["place_id"], "n": p_["place_name"], "k": p_["place_kind"], "a": p_["local_area"] or "",
                    "lat": p_["lat"], "lon": p_["lon"], "nc": p_["n_council"], "np": p_["n_permits"], "nk": p_["n_capital"],
                    "cap": p_["capital_new_budgets_cad"]} for p_ in places if p_["lat"] is not None],
    }
    items = {}
    for p_ in places:
        items[p_["place_id"]] = {**{k: v for k, v in p_.items() if not k.startswith("source_")},
                                 "facts": _json.loads(p_["facts"]) if p_["facts"] else None,
                                 "source": {"name": p_["source_name"], "source_url": p_["source_url"], "license": p_["source_license"], "as_of": p_["source_rows_updated_at"]}}
    log.data("places", len(places))
    return index, {**envelope(), "unit": "CAD", "items": items}


def build_housing_page(client, log) -> dict:
    b = rows(client, "mart_ca_vancouver_housing_buildings", "ORDER BY units DESC, place_name")
    r = rows(client, "mart_ca_vancouver_housing_rollups", "ORDER BY dimension, units DESC")
    if not (b and r):
        raise RuntimeError("a Vancouver housing mart is empty — run dbt build")
    roll: dict[str, list] = {}
    for x in r:
        roll.setdefault(x["dimension"], []).append({k: x[k] for k in ("key", "n_buildings", "units", "units_families", "units_seniors", "units_other", "n_splits_disagree")})
    completed = [x for x in b if x["project_status"] == "Completed"]
    log.data("housing buildings", len(b))
    return {
        **envelope(), "unit": "units",
        "source": {"name": b[0]["source_name"], "source_url": b[0]["source_url"], "license": b[0]["source_license"], "as_of": b[0]["source_rows_updated_at"]},
        "headline": {
            "n_buildings": len(b), "n_completed": len(completed), "units_completed": sum(x["units"] for x in completed),
            "units_all_stages": sum(x["units"] for x in b), "units_by_design_all_stages": sum(x["units_by_design"] or 0 for x in b),
            "n_splits_disagree": sum(1 for x in b if not x["splits_agree"]),
            "units_pipeline": sum(x["units"] for x in b if x["project_status"] != "Completed"),
            "first_occupancy_year": min((x["occupancy_year"] for x in completed if x["occupancy_year"]), default=None),
        },
        "rollups": roll,
        "buildings": [{"id": x["place_id"], "n": x["place_name"], "ad": x["address"], "a": x["local_area"] or "", "s": x["project_status"],
                       "y": x["occupancy_year"], "op": x["operator"], "u": x["units"], "uf": x["units_families"], "us": x["units_seniors"],
                       "uo": x["units_other"], "lat": x["lat"], "lon": x["lon"]} for x in b],
    }


def build_sources_page(client, log) -> dict:
    ds = rows(client, "mart_ca_vancouver_sources", "ORDER BY dataset_title")
    if not ds:
        raise RuntimeError("mart_ca_vancouver_sources is empty — run dbt build")
    log.data("portal datasets", len(ds))
    return {**envelope(), "datasets": [_strip(d, drop=()) | {"source_id": d["source_id"]} for d in ds]}


def build_drilldown_files(client, log) -> tuple[dict, dict]:
    """grant_categories.json (per category: year totals + recipients per year)
    and capital_programs.json (per program key: its row in every budget year)."""
    gc = rows(client, "mart_ca_vancouver_grant_category_payees", "ORDER BY category, fiscal_year, amount_cad DESC")
    py = rows(client, "mart_ca_vancouver_capital_program_years", "ORDER BY program_key, budget_year")
    if not (gc and py):
        raise RuntimeError("a Vancouver drill-down mart is empty — run dbt build")
    cats: dict[str, dict] = {}
    for r in gc:
        c = cats.setdefault(r["category"], {"category": r["category"], "years": {}})
        y = c["years"].setdefault(str(r["fiscal_year"]), {"total_cad": 0.0, "n_lines": 0, "withheld_cad": 0.0, "recipients": []})
        y["total_cad"] += r["amount_cad"]; y["n_lines"] += r["n_lines"]
        if r["payee_key"] is None:
            y["withheld_cad"] += r["amount_cad"]
        else:
            # [key, name, $, has detail page]
            y["recipients"].append([r["payee_key"], r["payee_name"], r["amount_cad"], r["has_fiche"]])
    progs: dict[str, dict] = {}
    for r in py:
        pr = progs.setdefault(r["program_key"], {"program_key": r["program_key"], "years": []})
        pr["program_name"] = r["program_name"]          # ordered by year: the latest spelling wins
        pr["service_category_1"] = r["service_category_1"]
        pr["service_category_2"] = r["service_category_2"]
        pr["years"].append({k: r[k] for k in r if k not in ("program_key",)})
    log.data("grant categories", len(cats)); log.data("capital programs (keyed)", len(progs))
    return ({**envelope(), "unit": "CAD", "items": cats}, {**envelope(), "unit": "CAD", "items": progs})


def build_records_files(client, log, out: Path) -> tuple[dict, dict]:
    """Council's decisions as a downloadable dataset: two gzipped CSVs (items;
    item × parcel), a preview, schema.json (what each column means) and
    stats.json (every count the records page prints, counted over the rows
    written here, so page and download cannot disagree)."""
    import csv
    import gzip
    import io
    from collections import Counter

    items = rows(client, "mart_ca_vancouver_city_council_items", "ORDER BY vote_date DESC, item_key")
    if not items:
        raise RuntimeError("mart_ca_vancouver_city_council_items is empty — run dbt build")
    src = next((x for x in items[0]["sources"] if x["source_id"] == "council_voting_records"), items[0]["sources"][0])
    SUBJECTS = [
        ("s_rezoning", "Rezoning & development", "rezon|cd-1|text amendment|official development plan|odp|zoning"),
        ("s_housing", "Housing", "housing|rental|tenant|renovict|affordab|shelter|modular|homeless"),
        ("s_heritage", "Heritage", "heritage"),
        ("s_licences", "Liquor & business licences", "liquor|licence|license|patio|cannabis|brewery|lounge"),
        ("s_land", "Land, lanes & leases", "closure and sale|lane|road closure|sale of|lease|acquisition|easement|encroachment"),
    ]
    item_cols = [
        ("item_key", "stable key of the item in the City's voting record"),
        ("vote_date", "date council voted"), ("vote_year", "year council voted"),
        ("meeting_type", "the meeting, as the City names it (Regular Council, Public Hearing, …)"),
        ("meeting_id", "the City's meeting identifier"),
        ("title", "the agenda text with its agenda numbering removed"),
        ("text", "the agenda text as the City published it (UTF-8 misreadings repaired)"),
        ("decision", "the decision as the City recorded it"),
        ("outcome_line", "the decision with its tally: \"Carried unanimously\", \"Carried 6–5\", \"Lost 3–8\""),
        ("n_in_favour", "councillors recorded in favour"), ("n_opposition", "councillors recorded in opposition"),
        ("n_conflicts", "councillors who declared a conflict"), ("n_absent", "councillors recorded absent"),
        ("is_contested", "true when at least one councillor voted each way"),
        *[(k, f"subject keyword rule: agenda text matches {rule}") for k, _, rule in SUBJECTS],
        ("n_places", "parcels the agenda text names that were found in the property-address register"),
    ]
    place_cols = [
        ("item_key", "the item (joins the decisions table)"),
        ("civic_number", "the register's civic number the item was placed on"),
        ("std_street", "the register's street name"),
        ("rule", "how it was placed: exact (the number exists), range (inside a range the text names), hundred_block (same street, same hundred block)"),
        ("lon", "parcel centroid longitude (WGS84)"), ("lat", "parcel centroid latitude (WGS84)"),
        ("block_idx", "the city block containing the parcel (row of the block outlines, stable within a build)"),
    ]

    def gz_csv(path: Path, header: list[str], data: list[list]) -> int:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(header)
        w.writerows(data)
        raw = buf.getvalue().encode("utf-8")
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb", compresslevel=9) as f:
            f.write(raw)
        return path.stat().st_size

    rec = out / "records"
    item_rows = [[it["item_key"], it["vote_date"], it["vote_year"], it["meeting_type"], it["meeting_id"], it["title"], it["text"],
                  it["decision"], it["outcome_line"], it["n_in_favour"], it["n_opposition"], it["n_conflicts"], it["n_absent"],
                  it["is_contested"], *[it[k] for k, _, _ in SUBJECTS], len(it["places"] or [])] for it in items]
    place_rows = [[it["item_key"], p_["civic_number"], p_["std_street"], p_["rule"], p_["lon"], p_["lat"], p_["block_idx"]]
                  for it in items for p_ in (it["places"] or [])]
    size = gz_csv(rec / "vancouver_council_decisions.csv.gz", [c for c, _ in item_cols], item_rows)
    gz_csv(rec / "vancouver_council_decision_places.csv.gz", [c for c, _ in place_cols], place_rows)
    prev = io.StringIO()
    w = csv.writer(prev)
    w.writerow([c for c, _ in item_cols])
    w.writerows(item_rows[:200])
    (rec / "vancouver_council_decisions.preview.csv").write_text(prev.getvalue(), encoding="utf-8")

    def outcome(it: dict) -> str:
        d = (it["decision"] or "").lower()
        if d.startswith("carried unanimously"):
            return "Carried unanimously"
        if d.startswith("carried"):
            return "Carried on a split vote" if it["n_opposition"] else "Carried"
        if d.startswith("lost") or d.startswith("defeated"):
            return "Lost"
        return it["decision"] or "Not recorded"

    by_year: dict[int, list[int]] = {}
    for it in items:
        y = by_year.setdefault(it["vote_year"], [0, 0])
        y[0] += 1
        y[1] += 1 if it["places"] else 0
    years = sorted(y for y in by_year if y is not None)
    stats = {
        "rows": len(items),
        "years": [years[0], years[-1]],
        "placed_items": sum(1 for it in items if it["places"]),
        "place_rows": len(place_rows),
        "contested": sum(1 for it in items if it["is_contested"]),
        "download_bytes": size,
        "by_year": [[y, by_year[y][0], by_year[y][1]] for y in years],
        "meeting_type": Counter(it["meeting_type"] or "Not recorded" for it in items).most_common(),
        "outcome": Counter(outcome(it) for it in items).most_common(),
        "subject": [[label, sum(1 for it in items if it[k])] for k, label, _ in SUBJECTS],
        "placement_rule": Counter(p_[3] for p_ in place_rows).most_common(),
    }
    schema = {
        "name": f"Vancouver city council decisions, {years[0]}–{years[-1]}",
        "description": "Every item Vancouver city council voted on, with its outcome and tally, keyword subjects, and the parcels its agenda text names.",
        "counting_rules": [
            "One row of the decisions table is one item council voted on (the voting record's item key). Councillors' individual votes are not in these files: tallies only.",
            "Subjects are keyword rules over the City's own agenda text. An item can match several subjects or none, so subject counts do not add up to the rows.",
            "An item is placed when its agenda text names an address found in the City's property-address register — the exact number, a range, or the same street's hundred block. Nothing fuzzy: an item that cannot be placed stays unplaced.",
            "The places table has one row per item and parcel: an item naming a range of addresses has several rows.",
        ],
        "tables": [
            {"file": "vancouver_council_decisions.csv.gz", "grain": "one row per item voted on", "rows": len(item_rows),
             "columns": [{"name": c, "what": w_} for c, w_ in item_cols]},
            {"file": "vancouver_council_decision_places.csv.gz", "grain": "one row per item × parcel", "rows": len(place_rows),
             "columns": [{"name": c, "what": w_} for c, w_ in place_cols]},
        ],
        "source": {"name": src.get("name"), "source_url": src.get("url"), "license": src.get("license"), "as_of": src.get("as_of")},
    }
    log.data("records: items", len(item_rows)); log.data("records: item × parcel", len(place_rows))
    return schema, stats


def main() -> int:
    log = Logger("export_ca_vancouver")
    out = OUTPUT_DIR
    log.header(f"Export Vancouver ({MARTS_DATASET}) → {out}")
    client = get_bigquery_client()

    files: dict[str, str] = {}
    log.section("landing")
    write_json(out / "landing.json", build_landing(client, log)); files["landing.json"] = "mart_ca_vancouver_landing"
    log.section("contracts_top")
    write_json(out / "contracts_top.json", build_top_contracts(client, log)); files["contracts_top.json"] = "mart_ca_vancouver_top_contracts"
    log.section("votes_contested")
    write_json(out / "votes_contested.json", build_contested(client, log)); files["votes_contested.json"] = "mart_ca_vancouver_contested_votes"

    log.section("contracts pages")
    contracts, bids_file, vendors_file = build_contracts_pages(client, log)
    write_json(out / "contracts.json", contracts); files["contracts.json"] = "mart_ca_vancouver_{landing,contracts_years,vendors,bids}"
    write_json(out / "bids.json", bids_file); files["bids.json"] = "mart_ca_vancouver_{bids,contracts}"
    write_json(out / "vendors.json", vendors_file); files["vendors.json"] = "mart_ca_vancouver_{vendors,contracts}"

    log.section("finance pages")
    budget, debt = build_finance_pages(client, log)
    write_json(out / "budget.json", budget); files["budget.json"] = "mart_ca_vancouver_finance_{years,lines,sources}"
    write_json(out / "debt.json", debt); files["debt.json"] = "mart_ca_vancouver_finance_{years,lines,sources}"
    payments, payees_file = build_payments_pages(client, log)
    write_json(out / "payments.json", payments); files["payments.json"] = "mart_ca_vancouver_{payments_years,payees,finance_sources}"
    write_json(out / "payees.json", payees_file); files["payees.json"] = "mart_ca_vancouver_{payees,finance_sources}"
    write_json(out / "payroll.json", build_payroll_page(client, log)); files["payroll.json"] = "mart_ca_vancouver_payroll_{years,bands,departments}"

    log.section("capital, places, housing")
    write_json(out / "investments.json", build_investments_page(client, log)); files["investments.json"] = "mart_ca_vancouver_capital_{years,categories,plan,programs}"
    places_index, places_items = build_places_pages(client, log)
    write_json(out / "places.json", places_index); files["places.json"] = "mart_ca_vancouver_places"
    write_json(out / "place_items.json", places_items); files["place_items.json"] = "mart_ca_vancouver_places"
    write_json(out / "housing.json", build_housing_page(client, log)); files["housing.json"] = "mart_ca_vancouver_housing_{buildings,rollups}"

    write_json(out / "sources.json", build_sources_page(client, log)); files["sources.json"] = "mart_ca_vancouver_sources"
    grant_cats, cap_progs = build_drilldown_files(client, log)
    write_json(out / "grant_categories.json", grant_cats); files["grant_categories.json"] = "mart_ca_vancouver_grant_category_payees"
    write_json(out / "capital_programs.json", cap_progs); files["capital_programs.json"] = "mart_ca_vancouver_capital_program_years"

    rec_schema, rec_stats = build_records_files(client, log, out)
    write_json(out / "records" / "schema.json", rec_schema); files["records/schema.json"] = "mart_ca_vancouver_city_council_items"
    write_json(out / "records" / "stats.json", rec_stats); files["records/stats.json"] = "mart_ca_vancouver_city_council_items"

    write_json(out / "index.json", {**envelope(), "place": "ca/vancouver", "schema_family": "ca-municipal", "files": files})
    log.success(f"wrote {len(files) + 1} files", extra=str(out))
    log.summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
