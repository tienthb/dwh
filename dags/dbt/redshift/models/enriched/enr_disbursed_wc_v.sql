{{ config(
    materialized="view",
    alias="enr_disbursed_wc_v",
    tags=["salesforce", "enriched"]
) }}

SELECT 
    disbursed_date,
    sub_product_type,
    COUNT(1) AS n_disbursed,
    SUM(disbursed_amount) AS disbursed_amount
    SUM(CASE WHEN DATE_TRUNC('month', application_date) != DATE_TRUNC('month', disbursed_date) THEN 1 END) AS n_disbursal_from_last_month
FROM {{ ref('cfm_wc_loan_t') }}
WHERE is_disbursed = 1
GROUP BY disbursed_date, sub_product_type