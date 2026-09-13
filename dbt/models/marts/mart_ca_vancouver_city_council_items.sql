-- =============================================================================
-- Mart: the map's council record — one row per item, with every parcel it is
-- placed on (centroid lon/lat, the block containing it, and the placement
-- rule), plus provenance. Read by
-- the map's decision layer.
-- =============================================================================

{{ config(materialized='table', schema='ca_marts', tags=['ca', 'marts', 'map']) }}

WITH cat AS (
    SELECT ARRAY_AGG(STRUCT(source_id, dataset_title AS name, dataset_page_url AS url,
                            license_title AS license, rows_updated_at AS as_of)) AS sources
    FROM {{ ref('core_ca_vancouver_source_catalog') }}
    WHERE source_id IN ('council_voting_records', 'property_addresses', 'property_parcel_polygons')
),

places AS (
    SELECT
        pl.item_key,
        ARRAY_AGG(STRUCT(p.lon AS lon, p.lat AS lat, b.block_idx AS block_idx, pl.rule AS rule,
                         pl.civic_number AS civic_number, pl.std_street AS std_street)
                  ORDER BY pl.std_street, pl.civic_number) AS places
    FROM {{ ref('stg_ca_vancouver_council_item_places') }} pl
    JOIN {{ ref('core_ca_vancouver_parcels') }} p ON p.tax_coord = pl.tax_coord
    LEFT JOIN {{ ref('core_ca_vancouver_blocks') }} b ON ST_CONTAINS(b.geog, ST_GEOGPOINT(p.lon, p.lat))
    GROUP BY pl.item_key
)

SELECT i.*, COALESCE(pl.places, []) AS places, cat.sources
FROM {{ ref('core_ca_vancouver_council_items') }} i
LEFT JOIN places pl USING (item_key)
CROSS JOIN cat
