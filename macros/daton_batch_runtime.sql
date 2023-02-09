{% macro daton_batch_runtime() %}

    {% if target.type =='snowflake' %}
        daton_batch_runtime as _daton_batch_runtime
    {% else %}
        _daton_batch_runtime
    {% endif %}

{% endmacro %}
