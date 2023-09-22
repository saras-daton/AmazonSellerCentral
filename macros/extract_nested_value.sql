{% macro extract_nested_value(variable1, variable2, variable3) %}

    {% if target.type =='snowflake' %}
    {{variable1}}.value:{{variable2}}::{{variable3}}
    {% else %}
    cast({{variable1}}.{{variable2}} as {{variable3}})
    {% endif %}
    
{% endmacro %}