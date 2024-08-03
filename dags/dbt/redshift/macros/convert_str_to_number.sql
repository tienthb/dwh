{% macro convert_str_to_number(columns, number_type) %}
    {% set result = [] %}
    
    {% if number_type.startswith("int") %}
        {% set cast_stmt = '::float' %}
    {% else %}
        {% set cast_stmt = '' %}
    {% endif %}

    {% for col in columns.split(",\n") %}
        {% do result.append("CASE WHEN REGEXP_INSTR({0}, '^[-+]?[0-9]*\.?[0-9]+$') > 0 THEN {0}{1}::{2} END AS {0}".format(col.strip(), cast_stmt, number_type)) %}
    {% endfor %}
    {{ result | join(",\n") }}    
{% endmacro %}