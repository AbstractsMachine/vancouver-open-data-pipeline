-- =============================================================================
-- Mart: non-market housing rollups for the housing page, one long table —
-- dimension ∈ status | decade | local_area | operator — with buildings and
-- units (clientele count) by families / seniors / other. decade, local_area
-- and operator cover COMPLETED buildings only (homes that exist); status
-- covers all four stages.
-- =============================================================================

{{ config(materialized='table', schema='ca_marts', tags=['ca', 'marts']) }}

WITH b AS (SELECT * FROM {{ ref('mart_ca_vancouver_housing_buildings') }}),

long AS (
    SELECT 'status' AS dimension, project_status AS key, * FROM b
    UNION ALL
    SELECT 'decade', CAST(DIV(occupancy_year, 10) * 10 AS STRING), * FROM b WHERE project_status = 'Completed' AND occupancy_year IS NOT NULL
    UNION ALL
    SELECT 'local_area', COALESCE(local_area, 'Not located'), * FROM b WHERE project_status = 'Completed'
    UNION ALL
    SELECT 'operator', COALESCE(operator, 'Not given'), * FROM b WHERE project_status = 'Completed'
)

SELECT
    dimension,
    key,
    COUNT(*)            AS n_buildings,
    SUM(units)          AS units,
    SUM(units_families) AS units_families,
    SUM(units_seniors)  AS units_seniors,
    SUM(units_other)    AS units_other,
    COUNTIF(NOT splits_agree) AS n_splits_disagree
FROM long
GROUP BY 1, 2
