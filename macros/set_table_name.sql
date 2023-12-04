{% macro set_table_name(tbl_ptrn, exclude_tbl_ptrn) %}

{% set table_name_query %}
    {# /*For table patterns */#}
    {% if target.type =='snowflake' %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from INFORMATION_SCHEMA.TABLES 
    where lower(table_name) like '{{var(tbl_ptrn)}}'  and table_schema='{{ var("raw_schema") }}'
    {% else %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from {{ var('raw_database') }}.{{ var('raw_schema') }}.INFORMATION_SCHEMA.TABLES
    where lower(table_name) like '{{var(tbl_ptrn)}}'
    {% endif %}

{#/* if exclude_tbl_ptrn is null as split can't have null value */#}
{% if var(exclude_tbl_ptrn) is none %}
    {% set  exclude_tbl= [] %}
{% else %}
    {% set  exclude_tbl= var(exclude_tbl_ptrn).split(', ') %}
    {% for each_table in exclude_tbl %}
        {# /*For exculde table patterns */#}
            and lower(table_name) not like '{{each_table}}'
    {% endfor %}
{% endif%}

{% endset %} 

    {# /*For list of tables to iterate*/ #}
{% set results = run_query(table_name_query) %}
    {% if execute %}

        {% set results_list = results.columns[0].values() %}
        {{ return(results_list) }} {# /*macro will return results_list*/ #}
    {% else %}
        {% set results_list = [] %}
        {{ return(results_list) }}
    {% endif %}

{% endmacro %}