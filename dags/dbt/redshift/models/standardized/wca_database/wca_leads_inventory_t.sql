{{ config(
    materialized="table",
    on_schema_change="append_new_columns",
    unique_key="postman_id",
    alias="wca_leads_inventory_t",
    tags=["wca_database"],
    enabled=False
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'modified_at' -%}
{%- set column_list = 'batch_id::INT AS batch_id,
    postman_id::INT as postman_id,
    loan_app_id::INT AS loan_app_id,
    is_published::SMALLINT::BOOL AS is_published,
    uen_number,
    borrower_name,
    topup_limit::DECIMAL(20, 4) AS topup_limit' -%}

{{ gen_sql(unique_key, incr_key, column_list, "wca", "source.wca_full_leads_inventory") | trim }}


