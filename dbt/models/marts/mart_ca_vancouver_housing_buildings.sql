-- =============================================================================
-- Mart: non-market housing, one row per building the City lists (641), typed
-- out of the place's published facts: status, occupancy year, operator, units
-- by clientele and by bedroom count, accessible units, local area, place_id
-- (the place detail page). units = families + seniors + other clientele.
-- The register splits the same homes a second way, by design (studio, room,
-- 1–4 bedrooms), and the two counts DISAGREE for 36 of 641 buildings (40,658
-- by clientele vs 40,709 by design, measured 2026-09-13) — e.g. a hotel
-- listing 41 rooms and 0 clientele units. Both travel: units_by_design and
-- splits_agree; the output reports the gap instead of choosing.
-- =============================================================================

{{ config(materialized='table', schema='ca_marts', tags=['ca', 'marts']) }}

SELECT
    place_id,
    place_name,
    address,
    local_area,
    lon, lat, url, block_idx,
    JSON_VALUE(facts, '$.project_status')                          AS project_status,
    SAFE_CAST(JSON_VALUE(facts, '$.occupancy_year') AS INT64)      AS occupancy_year,
    JSON_VALUE(facts, '$.operator')                                AS operator,
    SAFE_CAST(JSON_VALUE(facts, '$.clientele_families') AS INT64)  AS units_families,
    SAFE_CAST(JSON_VALUE(facts, '$.clientele_seniors') AS INT64)   AS units_seniors,
    SAFE_CAST(JSON_VALUE(facts, '$.clientele_other') AS INT64)     AS units_other,
    COALESCE(SAFE_CAST(JSON_VALUE(facts, '$.clientele_families') AS INT64), 0)
      + COALESCE(SAFE_CAST(JSON_VALUE(facts, '$.clientele_seniors') AS INT64), 0)
      + COALESCE(SAFE_CAST(JSON_VALUE(facts, '$.clientele_other') AS INT64), 0) AS units,
    SAFE_CAST(JSON_VALUE(facts, '$.units_studio') AS INT64)        AS units_studio,
    SAFE_CAST(JSON_VALUE(facts, '$.units_room') AS INT64)          AS units_room,
    SAFE_CAST(JSON_VALUE(facts, '$.units_1br') AS INT64)           AS units_1br,
    SAFE_CAST(JSON_VALUE(facts, '$.units_2br') AS INT64)           AS units_2br,
    SAFE_CAST(JSON_VALUE(facts, '$.units_3br') AS INT64)           AS units_3br,
    SAFE_CAST(JSON_VALUE(facts, '$.units_4br') AS INT64)           AS units_4br,
    SAFE_CAST(JSON_VALUE(facts, '$.units_accessible') AS INT64)    AS units_accessible,
    SAFE_CAST(JSON_VALUE(facts, '$.units_studio') AS INT64) + SAFE_CAST(JSON_VALUE(facts, '$.units_room') AS INT64)
      + SAFE_CAST(JSON_VALUE(facts, '$.units_1br') AS INT64) + SAFE_CAST(JSON_VALUE(facts, '$.units_2br') AS INT64)
      + SAFE_CAST(JSON_VALUE(facts, '$.units_3br') AS INT64) + SAFE_CAST(JSON_VALUE(facts, '$.units_4br') AS INT64) AS units_by_design,
    (COALESCE(SAFE_CAST(JSON_VALUE(facts, '$.clientele_families') AS INT64), 0)
      + COALESCE(SAFE_CAST(JSON_VALUE(facts, '$.clientele_seniors') AS INT64), 0)
      + COALESCE(SAFE_CAST(JSON_VALUE(facts, '$.clientele_other') AS INT64), 0))
    = (SAFE_CAST(JSON_VALUE(facts, '$.units_studio') AS INT64) + SAFE_CAST(JSON_VALUE(facts, '$.units_room') AS INT64)
      + SAFE_CAST(JSON_VALUE(facts, '$.units_1br') AS INT64) + SAFE_CAST(JSON_VALUE(facts, '$.units_2br') AS INT64)
      + SAFE_CAST(JSON_VALUE(facts, '$.units_3br') AS INT64) + SAFE_CAST(JSON_VALUE(facts, '$.units_4br') AS INT64)) AS splits_agree,
    n_council, n_permits, n_heritage,
    source_name, source_url, source_license, source_rows_updated_at
FROM {{ ref('mart_ca_vancouver_places') }}
WHERE place_kind = 'non_market_housing'
