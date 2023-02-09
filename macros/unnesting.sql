{% macro unnesting(variable) %}

    {% if target.type =='snowflake' %}
    , LATERAL FLATTEN( input => PARSE_JSON({{variable}})) {{variable}}
    {% else %}
    left join unnest({{variable}}) {{variable}}
    {% endif %}
    
{% endmacro %}