-- =============================================================================
-- Mart: the city map's parks — row-level, exactly the core rows, with the
-- provenance of every source that fed them stamped from the catalog. A map-ready layer.
-- 
-- =============================================================================

{{ config(materialized='table', schema='ca_marts', tags=['ca', 'marts', 'map']) }}

WITH cat AS (
    SELECT
        ARRAY_AGG(STRUCT(source_id, dataset_title AS name, dataset_page_url AS url,
                         license_title AS license, rows_updated_at AS as_of)
                  ORDER BY source_id) AS sources
    FROM {{ ref('core_ca_vancouver_source_catalog') }}
    WHERE source_id IN ('parks_polygons')
)

SELECT c.*, cat.sources
FROM {{ ref('core_ca_vancouver_parks') }} c
CROSS JOIN cat
