--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('CatalogItemsSummary', True)) }}

{{config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by = ['ReferenceASIN'],
    unique_key = ['brandName','ReferenceASIN'])}}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT MAX(_daton_batch_runtime) - 2592000000 FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%catalogitemssummary' 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% if var('timezone_conversion_flag') %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}


{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}
    SELECT * except(row_num)
    From (
        select 
        CAST(RequeststartDate as timestamp) RequeststartDate,
        CAST(RequestendDate as timestamp) RequestendDate,
        ReferenceASIN,
        marketplaceId,
        brandName,
        browseNode,
        colorName,
        itemName,
        manufacturer,
        modelNumber,
        sizeName,
        styleName,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
            DATETIME_ADD(TIMESTAMP_MILLIS(cast(_daton_batch_runtime as int)), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
            TIMESTAMP_MILLIS(cast(_daton_batch_runtime as int)) _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        Dense_Rank() OVER (PARTITION BY brandName,ReferenceASIN order by _daton_batch_runtime desc, _daton_batch_id desc) row_num
	    from {{i}}    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}    
        ) 
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
