{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="master_id",
    alias="wca_master_details_t",
    tags=["wca_database"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = "updated_date" -%}
{%- set column_list = "application_status,
    bank_statement_urls,
    brcs_report,
    brcs_response,
    brcs_status,
    cbs_flag,
    client_transaction_id,
    created_date,
    master_id,
    partnership,
    perfios_json_report_url,
    perfios_report,
    perfios_response,
    perfios_status,
    perfios_transaction_id,
    registration_details,
    share_holder_details,
    share_holder_names,
    uen_number,
    vendor_id"
-%}

{% if is_incremental() %}
    {% set src_table = "source.wca_cdc_wca_master_details" %}
{% else %}
    {% set src_table = "source.wca_full_wca_master_details" %}
{% endif %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}
