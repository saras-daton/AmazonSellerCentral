{% macro exclude() %}

    {% if var('snowflake_database_flag') %}
        exclude
    {% else %}
        except
    {% endif %}

{% endmacro %}
