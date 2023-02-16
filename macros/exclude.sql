{% macro exclude() %}

    {% if target.type =='snowflake' %}
        exclude
    {% else %}
        except
    {% endif %}

{% endmacro %}
