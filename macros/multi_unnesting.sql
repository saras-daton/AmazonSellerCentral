{% macro multi_unnesting(variable,variable1) %}
    {% if var('snowflake_database_flag') %}
    , LATERAL FLATTEN( input => PARSE_JSON({{variable}}.VALUE:{{variable1}})) {{variable1}}
    {% else %}
    left join unnest({{variable}}.{{variable1}}) {{variable1}}
    {% endif %}
    
{% endmacro %}