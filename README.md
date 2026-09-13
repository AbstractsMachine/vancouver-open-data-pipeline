# Vancouver open-data pipeline

A reproducible pipeline for the City of Vancouver's public-money data: what the
City spends and raises, what it owes, whom it pays, what it builds, and the
decisions its council makes — loaded from public sources, checked against the
City's own printed totals, and exported as open JSON.

Everything here is built from data the City and the Government of Canada
already publish. Nothing is hand-typed: every figure comes from a source
recorded with its URL.

## Sources

| source | what | how it is read |
|---|---|---|
| [City of Vancouver open-data portal](https://opendata.vancouver.ca) | 36 datasets: council voting records, awarded contracts, employee remuneration over $75,000, capital budgets 2021–2026 and the 2023–2026 Capital Plan, assessment roll, parcels, addresses, building permits, footprints, streets, trees, parks, community facilities, non-market housing, heritage register… | Opendatasoft Explore API v2.1, config only (`config/vancouver.yaml`) |
| Statements of Financial Information 2011–2025 (PDF) | payments to suppliers over $25,000, grants, the five-year statistical review (revenue, expenses, debt, reserves, balance sheet) | Internet Archive copies (vancouver.ca refuses scripted requests); parsed from the PDF text |
| [NRCan HRDEM](https://open.canada.ca/data/en/dataset/957782bf-847c-4644-a757-e383c0057995) | 1 m LiDAR surface and terrain models (2016) | windowed reads of cloud-optimised GeoTIFFs, to measure building heights |

Licences: City data — [Open Government Licence – Vancouver](https://opendata.vancouver.ca/pages/licence/); NRCan data — Open Government Licence – Canada. The code in this repository is MIT-licensed.

## Layout

```
config/vancouver.yaml      portal sources (dataset ids, target tables, ODSQL clauses)
pipeline/
  common.py                environment, BigQuery client, logging
  sync/                    portal → BigQuery raw tables + catalog snapshot
  finance/                 SOFI PDFs: fetch from the Internet Archive, parse schedules and the five-year review
  city/                    LiDAR building heights; council agenda addresses → parcels
  export/                  marts → JSON files in outputs/
dbt/                       staging → analytics (row-level) → marts (rollups), seeds, tests
docs/data-notes.md         what the sources are really like, and the rule followed for each finding
reports/                   weekly progress reports
```

## Run it

Requirements: Python 3.11+, the `pdftotext` binary (poppler), a Google Cloud
project with BigQuery, and `gcloud auth application-default login`.

```bash
cp .env.example .env          # set GCP_PROJECT, datasets, location
cp dbt/profiles.yml.example dbt/profiles.yml
make install
make sync                     # portal datasets → $VAN_RAW_DATASET
make sofi-fetch sofi-parse review
make dbt-deps seed build
make council heights build    # derived sources, then rebuild what reads them
make export                   # JSON in outputs/
```

## Checks

`dbt build` runs the data tests. Beyond keys and accepted values, they check:

- every section of the five-year review adds up to the total printed under it, and revenue − expenses equals the printed surplus;
- expenses by object and by function have the same total every year; the debt table's own arithmetic holds;
- each supplier and grant schedule adds up to its printed total within $50, or the difference is listed in `dbt/seeds/seed_ca_vancouver_sofi_known_gaps.csv` with what was measured;
- contract totals agree between response and competition grain;
- capital budget and plan totals survive from the raw files to the marts;
- no withheld name reaches the payee outputs, and no payroll figure covers fewer than 10 people where it would be one person's pay;
- an address point joins at most one city block.

## Privacy

The remuneration schedule and the payment schedules contain people's names. Names are kept in the warehouse for grain integrity only: marts and outputs publish counts and totals. A payee name that could be a person's (no organisation marker) is counted in every total and not published.

## Status

Built September 2026. See `reports/` for weekly progress and `docs/data-notes.md` for findings.
