{% macro from_epoch_milliseconds() %}

    {% if var('snowflake_database_flag') %}
        to_timestamp_ltz(cast(daton_batch_runtime as int))
    {% else %}
        TIMESTAMP_MILLIS(cast(_daton_batch_runtime as int))
    {% endif %}

{% endmacro %}
