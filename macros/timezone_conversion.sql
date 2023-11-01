{% macro timezone_conversion(col_name) %}
 
    {% if target.type =='snowflake' %}
        cast(CONVERT_TIMEZONE('{{var("to_timezone")}}', {{col_name}}::timestamp_ntz) as {{ dbt.type_timestamp() }})
    {% else %}
        DATETIME(cast({{col_name}} as timestamp), '{{var("to_timezone")}}')
       
    {% endif %}
 
{% endmacro %}