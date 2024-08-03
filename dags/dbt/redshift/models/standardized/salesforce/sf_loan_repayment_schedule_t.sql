{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="sf_loan_repayment_schedule_t",
    tags=["salesforce"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'lastmodifieddate' -%}
{%- set column_list = 'id,
    genesis_product__c,
    is_monthly_fees_paid__c,
    late_charge__c,
    loan__archived__c,
    loan__balance__c,
    loan__due_date__c,
    loan__due_interest__c,
    loan__due_interest_on_overdue__c,
    loan__due_principal__c,
    loan__id__c,
    loan__is_billed__c,
    loan__ispaid__c,
    loan__loan_account__c,
    loan__paid_fees__c,
    loan__paid_interest__c,
    loan__paid_interest_on_overdue__c,
    loan__paid_principal__c,
    loan__paid_total__c,
    loan__past_due_date__c,
    loan_id__c,
    loan__date_paid__c,
    loan__loan_disbursal_transaction__c,
    loan__waived_interest__c,
    loan__total_due_fees__c,
    loan__due_amount__c,
    lastmodifieddate' -%}

{% set src_table = "source." ~ config.get("alias")[:-2] %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}
