{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="id",
    alias="sf_cl_contract_history_t",
    tags=["salesforce"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = 'createddate' -%}
{%- set column_list = 'id,
    createdbyid,
    createddate,
    field,
    parentid,
    newvalue,
    oldvalue' -%}

{% set src_table = "source." ~ config.get("alias")[:-2] %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}
