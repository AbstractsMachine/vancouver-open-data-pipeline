-- =============================================================================
-- Mart: capital expenditure budget and new multi-year budgets by service
-- category, per budget year. Categories are each plan's own words: a 2021
-- row ("One Water") and a 2026 row ("Water, sewers & drainage") are not the
-- same key — capital_plan travels with every row.
-- =============================================================================

{{ config(materialized='table', schema='ca_marts', tags=['ca', 'marts']) }}

SELECT
    budget_year,
    capital_plan,
    service_category_1,
    COUNT(*)                        AS n_lines,
    SUM(expenditure_budget_cad)     AS expenditure_budget_cad,
    SUM(new_multi_year_cad)         AS new_multi_year_cad,
    SAFE_DIVIDE(SUM(expenditure_budget_cad), SUM(SUM(expenditure_budget_cad)) OVER (PARTITION BY budget_year)) AS share_of_year
FROM {{ ref('core_ca_vancouver_capital_budget_lines') }}
GROUP BY 1, 2, 3
