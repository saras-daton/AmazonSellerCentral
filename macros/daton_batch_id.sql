{% macro daton_batch_id() %}

    {% if var('snowflake_database_flag') %}
        daton_batch_id as _daton_batch_id
    {% else %}
        _daton_batch_id
    {% endif %}

{% endmacro %}
