{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="sf_investor_fund_transaction_t",
    tags=["salesforce"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'lastmodifieddate' -%}
{%- set column_list = 'investor_id__c,
    id,
    account_created_date__c,
    amount_deposited__c,
    amount_withdrawn__c,
    loan__balance_after__c,
    cdate__c,
    loan__transaction_amount__c,
    correction_ift__c,
    createddate,
    withdraw__c,
    date_withdrawal__c,
    deposited_date1__c,
    ift_success__c,
    insured_loan_amount__c,
    insured_date_time__c,
    it_is_insured__c,
    lastmodifieddate,
    loan_app_id__c,
    loan__account__c,
    loan__archived__c,
    loan__cleared__c,
    loan__clearing_date__c,
    loan__reject_reason__c,
    loan__rejected__c,
    loan__reversed__c,
    loan__statement_date__c,
    loan__summary_record_id__c,
    loan__transaction_description__c,
    loan__transaction_type__c,
    loan__payment_mode__c,
    payment_type__c,
    peer__btransaction_hash__c,
    peer__bank_reference__c,
    peer__bank_statement_date__c,
    peer__bank_statement_exception__c,
    peer__booking_order__c,
    peer__investment_booking__c,
    peer__is_safe_funds_related__c,
    reason_for_correction__c,
    transaction_amount__c,
    loan__transaction_date__c,
    new_transaction_description__c,
    transactions_category__c,
    transactions_sub_category__c,
    type__c' -%}

{% set src_table = "source." ~ config.get("alias")[:-2] %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}

