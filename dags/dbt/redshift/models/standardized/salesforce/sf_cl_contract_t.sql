{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="sf_cl_contract_t",
    tags=["salesforce"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'lastmodifieddate' -%}
{%- set column_list = 'id,
    loan_app_id__c::INT AS loan_app_id__c,
    borrower_name__c,
    postman_id__c::INT AS postman_id__c,
    genesis_product__c,
    wc_subcategory_list__c,
    loan__loan_amount__c,
    actual_disbursal_amount__c,
    loan__disbursal_date__c,
    loan__application_date__c,
    loan__number_of_days_overdue__c,
    sector__c,
    net_annualized_irr__c::DECIMAL(10, 2) AS net_annualized_irr__c,
    monthly_interest_rate__c::DECIMAL(10, 2) AS monthly_interest_rate__c,
    disbursal_fee_percentage__c::DECIMAL(5, 2) AS disbursal_fee_percentage__c,
    genesis_tenure__c::SMALLINT AS genesis_tenure__c,
    loan__loan_status__c,
    loan__principal_remaining__c,
    loan__interest_remaining__c,
    loan__fees_remaining__c,
    loan__principal_paid__c,
    loan__interest_paid__c,
    loan__fees_paid__c,
    loan__last_transaction_timestamp__c,
    loan__charged_off_date__c,
    loan__charged_off_principal__c,
    loan__charged_off_interest__c,
    createddate,
    lastmodifieddate,
    genesis_app_id__c' -%}

{% set src_table = "source." ~ config.get("alias")[:-2] %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}

