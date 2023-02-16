{% macro daton_user_id() %}

    {% if target.type =='snowflake' %}
        daton_user_id
    {% else %}
        _daton_user_id
    {% endif %}

{% endmacro %}
