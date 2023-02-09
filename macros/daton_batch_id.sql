{% macro daton_batch_id() %}
    
    {% if target.type =='snowflake' %}
        daton_batch_id as _daton_batch_id
    {% else %}
        _daton_batch_id
    {% endif %}

{% endmacro %}
