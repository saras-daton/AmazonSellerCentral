{% macro extract_brand_and_store_name_from_table(table_name,position,flag,default_value) %}
    
    {%- if flag -%}
        {%- set name_parts = replace(table_name,'`','').split('.') -%}
        {%- if name_parts|length > 2 -%}
            {%- set extracted_name = name_parts[2].split('_')[position] -%}
        {% else %}
            {%- set extracted_name = default_value -%}
        {%- endif -%}
    {% else %}
        {%- set extracted_name = default_value -%}
    {%- endif -%}
    '{{ extracted_name }}'
{% endmacro %}