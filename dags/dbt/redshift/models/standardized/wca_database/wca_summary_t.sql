{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="master_id",
    alias="wca_summary_t",
    tags=["wca_database"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'id,updated_date' -%}
{%- set column_list = "id,
    master_id,
    decision,
    hard_rules,
    hard_rule_failure_reason,
    grad,
    CASE pricing WHEN '' THEN NULL ELSE pricing::float END AS pricing,
    company_name,
    CASE score WHEN '' THEN NULL ELSE score::float END AS score,
    CASE pre_limit12 WHEN '' THEN NULL ELSE pre_limit12::float END AS pre_limit12,
    calculated_limit12,
    uen,
    created_date,
    updated_date,
    bank_statement,
    cbs_failure_reason,
    fraud_rules,
    fraud_rule_failure_reason,
    brcs,
    cbs,
    bc_pd,
    experian_pd" -%}


{% if is_incremental() %}
    {% set src_table = "source.wca_cdc_summary" %}
{% else %}
    {% set src_table = "source.wca_full_summary" %}
{% endif %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}