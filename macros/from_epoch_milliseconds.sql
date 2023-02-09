{% macro from_epoch_milliseconds() %}

    {% if target.type =='snowflake' %}
        to_timestamp_ltz(cast(daton_batch_runtime as int))
    {% else %}
        TIMESTAMP_MILLIS(cast(_daton_batch_runtime as int))
    {% endif %}

{% endmacro %}
