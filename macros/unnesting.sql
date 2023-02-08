{% macro unnesting(variable) %}
    {% if var('snowflake_database_flag') %}
    , LATERAL FLATTEN( input => PARSE_JSON({{variable}})) {{variable}}
    {% else %}
    left join unnest({{variable}}) {{variable}}
    {% endif %}
    
{% endmacro %}