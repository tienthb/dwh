{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="cfm_wc_loan_t",
    tags=["salesforce", "conformed"],
    post_hook="""DELETE FROM conformed.cfm_wc_loan_t
        WHERE id IN (
        SELECT deal_app_id || '-'
        FROM conformed.cfm_wc_loan_t s
        GROUP BY deal_app_id
        HAVING COUNT(1) > 1 AND MIN(LENGTH(id)) = 19)"""
) }}

WITH loan_details AS 
(
	SELECT 
		cc.loan_app_id__c,
		cc.genesis_app_id__c,
		cc.loan__disbursal_date__c,
		cc.loan__loan_amount__c,
		cc.loan__loan_status__c,
		cc.borrower_name__c,
		cc.createddate,
		cc.lastmodifieddate,
        cc.sector__c
	FROM {{ ref('sf_cl_contract_t') }} cc
	WHERE cc.loan__loan_status__c NOT IN ('Partial Application', 'Canceled')
)
,base AS 
(
    SELECT 
        app_deal.id || '-' || COALESCE(app_loan.id, '') AS id,
        app_deal.id AS deal_app_id,
        app_deal.genesis__cl_product_name__c AS product_type,
        app_deal.postman_id__c::int AS postman_id__c,
        app_deal.wc_subcategory_list__c AS sub_product_type,
        CASE WHEN app_deal.wc_subcategory_list__c IN ('STP-WC (Company)', 'STC-WC (Sole Prop)') THEN 1 ELSE 0 END AS is_stp,
        app_deal.application_date__c AS application_date,
        cc.loan_app_id__c AS loan_id,
        cc.loan__disbursal_date__c AS disbursed_date,
        CASE 
            WHEN app_deal.genesis__status__c = 'REJECTED' OR app_deal.creditapprovalstatus__c = 'Reject' OR app_deal.sub_stage__c = 'Rejected' THEN 'Rejected'
            WHEN app_deal.genesis__status__c IN ('APPROVED', 'Credit Approval') OR app_deal.creditapprovalstatus__c = 'Approved' OR app_deal.sub_stage__c = 'Approved' THEN 'Approved'
            ELSE 'Pending'
        END AS decision,
        CASE WHEN cc.loan__disbursal_date__c IS NULL THEN 0 ELSE 1 END AS is_disbursed,
        cc.loan__loan_amount__c AS disbursed_amount,
        cc.loan__loan_status__c AS loan_status,
        cc.borrower_name__c AS borrower_name,
        GREATEST(app_deal.createddate, cc.createddate) AS createddate,
        GREATEST(app_deal.lastmodifieddate, cc.lastmodifieddate) AS lastmodifieddate,
        GETDATE() AS elt_updated_date,
        app_deal.recommended_limit__c,
        app_deal.application_approved_date__c,
        app_deal.rejection_date__c,
        app_deal.sub_stage__c,
        COALESCE(app_deal.sector__c, cc.sector__c, 'Missing') AS sector
    FROM {{ ref('sf_application_t') }} app_deal
        LEFT JOIN {{ ref('sf_application_t') }} app_loan ON app_deal.id = app_loan.genesis__parent_application__c
        LEFT JOIN loan_details cc ON app_loan.id = cc.genesis_app_id__c   
    {% if is_incremental() %}
        LEFT JOIN {{ this }} t ON app_deal.id = t.id
        WHERE GREATEST(app_deal.lastmodifieddate, cc.lastmodifieddate) > COALESCE(t.lastmodifieddate, '1970-01-01') 
    {%else%}
        WHERE 1 = 1 -- dummy condition 
    {% endif %}
        AND app_deal.genesis__cl_product_name__c = 'Working Capital Financing'
)
SELECT 
    *,
    CASE 
        WHEN decision = 'Pending' AND DATEDIFF(day, application_date, CONVERT_TIMEZONE('SGT', GETDATE())) > 10 THEN 1
        ELSE 0
    END AS is_drop,
    CASE decision WHEN 'Approved' THEN recommended_limit__c ELSE 0 END AS approved_limit,
    CASE 
        WHEN decision = 'Approved'
            AND is_disbursed = 0
            AND (
                sub_stage__c = 'Closed'
                OR DATEDIFF(DAY, CASE decision WHEN 'Approved' THEN application_approved_date__c ELSE rejection_date__c END, CONVERT_TIMEZONE('SGT', GETDATE())) > 14
            )
        THEN 'Reject by customer'
        ELSE ''
    END AS customer_decision
FROM base