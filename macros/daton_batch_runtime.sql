{% macro daton_batch_runtime() %}

    {% if var('snowflake_database_flag') %}
        daton_batch_runtime as _daton_batch_runtime
    {% else %}
        _daton_batch_runtime
    {% endif %}

{% endmacro %}
