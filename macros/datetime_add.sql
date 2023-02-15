{% macro datetime_add(var1, var2, var3) %}

    {% if target.type =='snowflake' %}
        TIMEADD( var3 , var2 , var1 )
    {% else %}
        DATETIME_ADD(var1, INTERVAL {{var2}} var3 )
    {% endif %}

{% endmacro %}
