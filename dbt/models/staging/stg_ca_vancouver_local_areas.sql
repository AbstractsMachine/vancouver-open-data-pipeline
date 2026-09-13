-- =============================================================================
-- Staging: the City's 22 local areas (planning areas).
--
-- Source: raw.ca_vancouver_local_area_boundary (ODS). `geom` is the ODS GeoJSON Feature serialised to a JSON
-- string by the adapter → `$.geometry` → GEOGRAPHY (make_valid). Map input
-- the map's layers are built from them in core/marts.
-- =============================================================================

{{ config(materialized='view', schema='ca_staging', tags=['ca', 'staging', 'map']) }}

SELECT
    NULLIF(TRIM(name), '')                              AS local_area,
    SAFE.ST_GEOGFROMGEOJSON(JSON_QUERY(SAFE_CAST(geom AS STRING), '$.geometry'), make_valid => TRUE) AS geog,
    _synced_at
FROM {{ source('ca_vancouver_raw', 'ca_vancouver_local_area_boundary') }}
