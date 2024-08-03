{% macro gen_wca_incr_cond() %}

    {% set clauses = [] %}

    {% set query %}

        WITH RECURSIVE params (last_updated, c_timestamp) AS 
        (
            SELECT MAX(elt_updated_date) AS last_updated, GETDATE() AS c_timestamp
            FROM {{ this }}
            UNION ALL
            SELECT DATEADD(day, 1, last_updated), c_timestamp
            FROM params
            WHERE last_updated < c_timestamp
        )
        SELECT 
            '(partition_0 = ''' || DATE_PART('year', last_updated)::varchar || ''' ' ||
            'AND partition_1 = ''' || RIGHT('0' + DATE_PART('month', last_updated)::varchar, 2) || ''' ' ||
            'AND partition_2 = ''' || RIGHT('0' + DATE_PART('day', last_updated)::varchar, 2) || ''')'
        FROM params

    {% endset %}

    {% set results = run_query(query) %}

    {% for row in results %}

        {% set clause = row[0] %}
        {% do clauses.append(clause) %}
        
    {% endfor %}

    WHERE {{ clauses | join(' OR ') }}
    
{% endmacro %}