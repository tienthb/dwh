{{ config(
    materialized="view",
    alias="enr_application_wc_v",
    tags=["salesforce", "enriched"]
) }}

WITH base AS 
(
	SELECT 
		application_date,
		sub_product_type,
		deal_app_id,
		COUNT(DISTINCT deal_app_id) AS n_submission,
	    COUNT(DISTINCT CASE decision WHEN 'Approved' THEN deal_app_id END) AS n_approval,
	    COUNT(DISTINCT CASE decision WHEN 'Rejected' THEN deal_app_id END) AS n_reject,
		AVG(approved_limit) AS approve_amount,
	    SUM(is_drop) AS n_drop,
	    SUM(CASE WHEN customer_decision = 'Reject by customer' THEN 1 END) AS n_customer_reject,
		AVG(CASE 
			WHEN 
				customer_decision != 'Reject by customer'
				AND decision = 'Approved'
				AND is_disbursed = 0
			THEN approved_limit END
		) AS pending_acceptance_amount,
		COUNT(DISTINCT CASE 
			WHEN 
				customer_decision != 'Reject by customer'
				AND decision = 'Approved'
				AND is_disbursed = 0
			THEN deal_app_id END
		) AS n_pending_acceptance
	FROM {{ ref('cfm_wc_loan_t') }}
	GROUP BY application_date, sub_product_type, deal_app_id
)
SELECT 
    application_date,
    sub_product_type,
    SUM(n_submission) AS n_submission,
    SUM(n_approval) AS n_approval,
    SUM(n_reject) AS n_reject,
    SUM(approve_amount) AS approve_amount,
    SUM(n_drop) AS n_drop,
    SUM(n_customer_reject) AS n_customer_reject,
	SUM(pending_acceptance_amount) AS pending_acceptance_amount,
	SUM(n_pending_acceptance) AS n_pending_acceptance
FROM base
GROUP BY application_date, sub_product_type
