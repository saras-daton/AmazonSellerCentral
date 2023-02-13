{% macro set_table_name(variable) %}

    {% if target.type =='snowflake' %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from INFORMATION_SCHEMA.TABLES 
    where lower(table_name) like '{{variable}}'  and table_schema='EDM'
    {% else %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES
    where lower(table_name) like '{{variable}}'
    {% endif %}

{% endmacro %}