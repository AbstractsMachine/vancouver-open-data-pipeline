-- =============================================================================
-- Mart: one row per capital program key × budget year — the program detail page:
-- categories as that year's file prints them, previously approved budget, the
-- new multi-year budget and (2023→) its funding split, total open budget,
-- spending to the prior year-end, budget available, and the year's
-- expenditure budget. Lines a file repeats for one program are summed.
-- =============================================================================

{{ config(materialized='table', schema='ca_marts', tags=['ca', 'marts']) }}

SELECT
    program_key,
    budget_year,
    ANY_VALUE(program_name)            AS program_name,
    ANY_VALUE(capital_plan)            AS capital_plan,
    ANY_VALUE(service_category_1)      AS service_category_1,
    ANY_VALUE(service_category_2)      AS service_category_2,
    ANY_VALUE(service_category_3)      AS service_category_3,
    COUNT(*)                           AS n_lines,
    SUM(previously_approved_cad)       AS previously_approved_cad,
    SUM(new_multi_year_cad)            AS new_multi_year_cad,
    SUM(new_pay_as_you_go_cad)         AS new_pay_as_you_go_cad,
    SUM(new_debt_cad)                  AS new_debt_cad,
    SUM(new_tax_fee_reserves_cad)      AS new_tax_fee_reserves_cad,
    SUM(new_development_reserves_cad)  AS new_development_reserves_cad,
    SUM(new_connections_cad)           AS new_connections_cad,
    SUM(new_partner_cad)               AS new_partner_cad,
    SUM(total_open_cad)                AS total_open_cad,
    SUM(spent_to_prior_year_end_cad)   AS spent_to_prior_year_end_cad,
    SUM(available_cad)                 AS available_cad,
    SUM(expenditure_budget_cad)        AS expenditure_budget_cad
FROM {{ ref('core_ca_vancouver_capital_budget_lines') }}
WHERE program_key != ''
GROUP BY 1, 2
