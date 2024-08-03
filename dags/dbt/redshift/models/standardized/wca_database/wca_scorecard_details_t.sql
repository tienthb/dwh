{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="wca_scorecard_details_t",
    tags=["wca_database"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = "event_timestamp" -%}
{%- set column_list = "master_id || '-' || module || '-' || variable AS id,
    created_date,
    master_id,
    module,
    score,
    value,
    variable"
-%}

{% if is_incremental() %}
    {% set src_table = "source.wca_cdc_scorecard_details" %}
{% else %}
    {% set src_table = "source.wca_full_scorecard_details" %}
{% endif %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}