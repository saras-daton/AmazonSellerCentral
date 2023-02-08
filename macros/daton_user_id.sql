{% macro daton_user_id() %}

    {% if var('snowflake_database_flag') %}
        daton_user_id as _daton_user_id
    {% else %}
        _daton_user_id
    {% endif %}

{% endmacro %}
