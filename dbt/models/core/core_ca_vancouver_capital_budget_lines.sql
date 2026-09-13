-- =============================================================================
-- Core: capital budget OBT — one row per program per budget year, 2021→2026.
--
-- program_key: ca_org_key over the program name, so "Britannia Renewal —
-- Phase 1" reads the same across budget years that re-spell it; a program is
-- followed across years only on an identical key.
-- The published files can list a program twice in one year, and carry rows
-- with NO program name that still hold money (2024 "Disposal": $10,305,000).
-- Rows are kept as published — totals include them; program_key is '' for an
-- unnamed row, so the program list leaves it out.
-- =============================================================================

{{ config(materialized='table', schema='ca_analytics', tags=['ca', 'core']) }}

WITH names AS (
    SELECT service_category_1 AS name, MAX(budget_year) AS last_year
    FROM {{ ref('stg_ca_vancouver_capital_budgets') }}
    WHERE service_category_1 IS NOT NULL
    GROUP BY 1
),

-- The 2026 files (budget and plan) cut long category names at 36 characters
-- ("Waste collection, diversion & dispos"); earlier budget files print them
-- whole. A name of 20+ characters that is the start of a longer name is
-- written out; then one casing per name ("Public Safety" / "Public safety"),
-- the latest year's.
expanded AS (
    SELECT n.name,
           COALESCE(ARRAY_AGG(f.name IGNORE NULLS ORDER BY LENGTH(f.name) DESC LIMIT 1)[SAFE_OFFSET(0)], n.name) AS full_name
    FROM names n
    LEFT JOIN names f
      ON LENGTH(n.name) >= 20 AND LENGTH(f.name) > LENGTH(n.name) AND STARTS_WITH(f.name, n.name)
    GROUP BY n.name
),

canon AS (
    SELECT LOWER(e.full_name) AS k, ARRAY_AGG(e.full_name ORDER BY n.last_year DESC, e.full_name LIMIT 1)[OFFSET(0)] AS best
    FROM expanded e JOIN names n USING (name)
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['budget_year', 'service_category_1', 'service_category_2', 'service_category_3', 'program_name', 'expenditure_budget_cad', 'new_multi_year_cad', 'previously_approved_cad']) }}
                             AS line_id,
    budget_year,
    capital_plan,
    COALESCE(c.best, b.service_category_1) AS service_category_1,
    service_category_2,
    service_category_3,
    program_name,
    COALESCE({{ ca_org_key('program_name') }}, '') AS program_key,
    previously_approved_cad,
    new_multi_year_cad,
    new_pay_as_you_go_cad,
    new_debt_cad,
    new_tax_fee_reserves_cad,
    new_development_reserves_cad,
    new_connections_cad,
    new_partner_cad,
    total_open_cad,
    spent_to_prior_year_end_cad,
    available_cad,
    expenditure_budget_cad,
    _synced_at
FROM {{ ref('stg_ca_vancouver_capital_budgets') }} b
LEFT JOIN expanded e ON e.name = b.service_category_1
LEFT JOIN canon c ON c.k = LOWER(e.full_name)
