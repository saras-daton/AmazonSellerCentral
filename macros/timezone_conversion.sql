{% macro timezone_conversion(col_name) %}
 
    {% if target.type =='snowflake' and '{{var(timezone_conversion_flag)}}' %}
        cast(CONVERT_TIMEZONE('{{var("to_timezone")}}', {{col_name}}::timestamp_ntz) as {{ dbt.type_timestamp() }})
    {% elif target.type =='bigquery' and '{{var(timezone_conversion_flag)}}' %}
        DATETIME(cast({{col_name}} as timestamp), '{{var("to_timezone")}}')
    {% elif target.type =='bigquery' %}
        DATETIME(cast({{col_name}} as timestamp))
    {% elif target.type =='snowflake' %}
        cast({{col_name}} as {{ dbt.type_timestamp() }})
    {% else %}
        {{col_name}}
    {% endif %}
 
{% endmacro %}