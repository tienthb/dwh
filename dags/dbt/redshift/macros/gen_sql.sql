{%- macro gen_sql(unique_key, incremental_keys, columns, src_type, src_table) %}

    {% set column_list = columns.split(",\n") %}
    {% set clean_column_list = [] %}
    {% for col in column_list %}
        {% if " AS " in col %}
            {% do clean_column_list.append("\n\t" ~ col.split(" ")[-1]) %}
        {% else %}
            {% do clean_column_list.append(col) %}
        {% endif %}
    {% endfor %}

    {% set incr_list = incremental_keys.split(",") %}
    {% set incr_conds = [] %}
    {% for col in incr_list %}
        {% set incr_cond = col.strip() ~ ' DESC' %}
        {% do incr_conds.append(incr_cond) %}
    {% endfor %}

    {% if not is_incremental() and src_type == "wca" %}
        {% set elt_updated_date = "'2023-05-15'::timestamp AS elt_updated_date" %} -- cdc ingestion start on 2023-05-15
    {% else %}
        {% set elt_updated_date = 'GETDATE() AS elt_updated_date' %}
    {% endif %}

    WITH base AS 
    (
        SELECT 
            {{ column_list | join(',') }}, --original columns
            {{ elt_updated_date }},
            ROW_NUMBER() OVER (PARTITION BY {{ unique_key }} ORDER BY {{ incr_conds | join(', ') }}) AS rn
        FROM {{ src_table }}
        {% if is_incremental() %}
            {% if src_type == "sf" %}
                {{ gen_sf_incr_cond() | trim }}
            {% elif src_type == "wca" %}
                {{ gen_wca_incr_cond() | trim }}
            {% endif %}
        {% else %}
            {% if src_table == "source.wca_full_log_report" %}
                WHERE master_id != 'nan'
            {% endif %}
        {% endif %}
    )
    SELECT 
        {{ clean_column_list | join(',') }},
        elt_updated_date
    FROM base
    WHERE rn = 1

{% endmacro -%}