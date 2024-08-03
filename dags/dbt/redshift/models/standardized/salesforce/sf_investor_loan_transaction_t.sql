{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="sf_investor_loan_transaction_t",
    tags=["salesforce"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'lastmodifieddate' -%}
{%- set column_list = 'id,
    loan_app_id__c,
    borrower_name__c,
    loan_amount__c,
    disbursal_fee__c,
    disbursed_amount__c,
    loan_maturity_date__c,
    reversed__c,
    total_amount_received__c,
    total_gst_on_validus_commission__c,
    validus_commission_from_interest__c,
    validus_commission_from_late_fee__c,
    loan__archived__c,
    loan__charged_off_date__c,
    loan__charged_off_fees__c,
    loan__charged_off_interest__c,
    loan__charged_off_principal__c,
    loan__late_fee_service_charge__c,
    loan__late_fees_paid__c,
    loan__principal_paid__c,
    loan__interest_paid__c,
    loan__loan_payment_transaction_time__c,
    loan__loan_payment_transaction__c,
    loan__tax__c,
    loan__txn_code__c,
    loan__reversed__c,
    loan__waived__c,
    reversal_date__c,
    createddate,
    lastmodifieddate,
    outstanding_amt__c' -%}

{% set src_table = "source." ~ config.get("alias")[:-2] %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}
