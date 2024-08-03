{{ config(
    materialized="incremental",
    on_schema_change="append_new_columns",
    unique_key="master_id",
    alias="wca_log_report_t",
    tags=["wca_database"]
) }}

{%- set unique_key = config.get('unique_key') -%}
{%- set incr_key = "id,updated_date" -%}
{%- set smallint_list = "brcs1,
    brcs10,
    brcs11,
    brcs12,
    brcs13,
    brcs14,
    brcs15,
    brcs16,
    brcs17,
    brcs18,
    brcs19,
    brcs1_1,
    brcs1_2,
    brcs2,
    brcs20,
    brcs3,
    brcs3_1,
    brcs4,
    brcs5,
    brcs6,
    brcs7,
    brcs8,
    brcs9,
    bri1,
    bri2,
    bri3,
    bri4,
    bri5,
    bri6,
    bri7,
    bs1,
    bs2,
    bs3,
    bs4,
    bs5,
    bs6,
    bs7,
    bs8,
    bs9,
    cbs1,
    cbs10,
    cbs2,
    cbs3_2,
    cbs4,
    cbs5_2,
    cbs6,
    cbs7_1,
    cbs7_2,
    cbs8,
    cbs9"
-%}
{%- set float_list = "brcs_score,
    bri_cbs_score,
    bs_score,
    limit12,
    limit18,
    pre_limit12,
    score"
-%}
{%- set int_list = "address_match,
    fid1_1,
    fid1_2,
    fid2,
    master_id,
    noa1,
    noa2"
-%}
{%- set column_list = """comments,
    company_name,
    created_date,
    fid_alert,
    id,
    industry_grade,
    pre_limit18,
    scenario,
    status,
    \"status date\",
    uen,
    vendor_id,
    updated_date"""
-%}

{% set column_list = column_list ~ ",\n\t" ~ convert_str_to_number(float_list, "float") | trim %}
{% set column_list = column_list ~ ",\n\t" ~ convert_str_to_number(int_list, "int") | trim %}
{% set column_list = column_list ~ ",\n\t" ~ convert_str_to_number(smallint_list, "int2") | trim %}

{% if is_incremental() %}
    {% set src_table = "source.wca_cdc_log_report" %}
{% else %}
    {% set src_table = "source.wca_full_log_report" %}
{% endif %}
{% set src_data = config.get("alias").split("_")[0] %}
{{ gen_sql(unique_key, incr_key, column_list, src_data, src_table) | trim }}