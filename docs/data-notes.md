# Data notes — what the Vancouver sources are really like

Measured while building this pipeline (September 2026). Each note says what was
found and the rule the code follows because of it. Newest at the bottom.

## City open-data portal (opendata.vancouver.ca)

| what was found | rule in the pipeline |
|---|---|
| The Opendatasoft export endpoint honours ODSQL `where` / `select`. `property-tax-report` is 1.55 M rows across 7 report years. | Sources may declare `where` / `select` in `config/vancouver.yaml`; the tax report is pulled one report year at a time. |
| Raw columns are typed by pandas inference: ISO dates arrive as strings, `geom` is a GeoJSON *Feature*. | Typing happens once, in staging: `JSON_QUERY(geom, '$.geometry')` → `ST_GEOGFROMGEOJSON(…, make_valid => TRUE)`. |
| The same field has two shapes: the records API returns `year` as an ISO timestamp, the export endpoint as a bare `'2025'`. Parsing only the first shape silently emptied a table. | Staging accepts both shapes. |
| The assessment roll's `pid` is nullable (545 of 226,360 rows in 2025, $2.7 bn of real properties); `land_coordinate` never is. | Join and key on `land_coordinate`; no `not_null` on `pid`. |
| 93,414 roll parcels vs 99,701 parcel polygons: about 6,300 polygons have no roll row (roads, parks, unassessed land). | Parcels LEFT JOIN the roll and carry `has_roll_row`. |
| Parcel polygons are polygon-grain: 3,032 parcels are multi-part (up to 63 pieces). | Parcels are aggregated to one row per `tax_coord` in core. |
| "Awarded contracts" is one row per BIDDER per competition: 6,414 rows = 1,671 competitions and 2,573 awards. Counting rows as contracts gave $4.43 B instead of $1.98 B. | Core is response-grain; totals use awards only. |
| 18 competitions print ONE amount, to the cent, beside 2–6 winners on the same date ($43,102,035 × 3 vehicle vendors): $128 M of repetition. | A shared amount is counted once in totals (`counted_amount_cad`) and flagged on each winner. |
| Vendor names vary in case and punctuation ("TELUS" / "Telus"). | `ca_org_key` macro: lower case, legal forms dropped, punctuation collapsed. |
| 2013–2014 competitions mostly list a single response (median 1 bidder, 3 from 2015). | Not a collapse in competition: the early register lists fewer responses. Outputs carry `comparable_from`. |
| `bid_number` is not a facet on the portal: `refine.bid_number` is silently ignored. | Links to the register search the quoted id (`table/?q="<bid_number>"`). |
| The 2015 building footprints (154,124) carry no height; the 2009 layer does, but is older. | Heights are measured from national LiDAR (below). |
| The public-trees register includes planting sites and stumps as assets. | Excluded in core. |
| The capital budget is one dataset per year with the YEAR IN THE COLUMN NAMES, and the columns changed with the 2023–2026 plan. Some rows have no program name but hold money (2024 "Disposal": $10.3 M). | Staging unpivots the six years to one shape; unnamed rows are kept in totals. |
| The 2026 capital files cut category names at 36 characters and change casing year to year; the 2025 file labels water rows with the previous plan's category name. | Truncated names are written out from earlier years; one casing per name; categories are never joined across plans. |
| The non-market housing register counts units twice (by clientele, by bedroom); they disagree for 36 of 641 buildings. | Both counts are kept with a `splits_agree` flag. |
| Linking by parcel can mislead on large sites: a library's parcel holds a whole mall redevelopment (32 properties, 169 permits). | Place outputs carry the parcel's number of properties so the caveat can be shown. |
| Council agenda text arrives as UTF-8 read as cp1252 ("â€“"). | Repaired before address matching. |

## Statements of Financial Information (PDF, via the Internet Archive)

| what was found | rule in the pipeline |
|---|---|
| vancouver.ca refuses scripted requests (403). The Internet Archive holds the same PDFs, 2011–2025. | `fetch_sofi.py` reads the archived bytes and records capture time, original URL and SHA-256 in a seed. |
| The five-year statistical review restates earlier years. Taking the newest print line by line mixed two revenue classifications in 2017–2020. | Each table-year is read whole from the newest report; a test proves every section adds up to its printed total. |
| The 2023–2025 reports print capital additions shifted one column (2022 = $585,399k, which is 2021's figure); the statements' own text says $455.0 M for 2022. | A misprint seed lists each case with its evidence; that table-year is read from the previous report. |
| "(Note 15)" in a label reads as a trailing number. | Note references are stripped after number parsing. |
| Supplier and grant schedules: 2021 prints Aramark's amount with its leading digits cut off (",541,678"); 2018 prints "15,0000" for a grant. | The typo is repaired only because the repaired figure makes the printed total add up exactly; the truncated amount is kept as NULL with its printed text. |
| 2013 "Other" grants and the 2018 and 2022 supplier schedules: every amount on the pages was read, none left over, and the lines still differ from the printed total. | Listed in `seed_ca_vancouver_sofi_known_gaps` with the measured difference; a test fails on any new gap. |
| A "could this be a person's name" rule based only on legal forms withheld $547 M, mostly organisations (a sewerage district abbreviated "Dist"). | Public bodies are matched first; generic markers widened; the class is called *withheld*, never *individuals* ($123 M). |

## Payroll (employees paid over $75,000)

| what was found | rule in the pipeline |
|---|---|
| The schedule names every employee. | Names never leave core; outputs are counts and totals. |
| A median over a small group is one person's pay (of 5, the third); a pay band holding one person totals that person's pay. | Medians and band totals are NULL under 10 people; roles held by fewer than 3 people are summed; a test enforces it. |
| The threshold has been $75,000 in every year 2008–2025. | Outputs note that part of the headcount growth is wages, not hiring. |

## National LiDAR (NRCan HRDEM)

| what was found | rule in the pipeline |
|---|---|
| The City's own LiDAR download host serves a browser challenge to scripts. NRCan republishes the region's LiDAR as 1 m cloud-optimised GeoTIFFs on S3 (`BC-Lower_Mainland_2016-1m` DSM + DTM). | `measure_building_heights.py` reads those files in windows; no bulk download. |
| Tower-on-podium: one outline holds both, so the median return is the podium (p50 101 m vs p90 196 m on one tower). | Height distribution (p50/p90 and levels) is kept per outline, not one number. |
| Shoreline is published as open lines; LiDAR "flat" also flags fields and runways outside the city. | Water is derived from the shoreline closed along the city limit, not from flatness alone. |

## BigQuery / dbt

| what was found | rule in the pipeline |
|---|---|
| BigQuery refuses correlated subqueries against another table in a SELECT list. | Pre-aggregate in a CTE and join. |

## Address register

- 78 address points lie on the shared edge of two block outlines. A point-in-polygon join puts them in both (99,822 rows from 99,744 points). `core_ca_vancouver_property_addresses` keeps each point once, on the smaller block; `assert_ca_address_points_one_block` checks the row count.
- 1,196 points carry no street name and 459 fall outside every block outline. Both are kept in the analytics table and left out of `mart_ca_vancouver_city_address_faces`.
- Ranges are kept per side of the street (odd / even numbers), so a number resolves to a block on its own side.
