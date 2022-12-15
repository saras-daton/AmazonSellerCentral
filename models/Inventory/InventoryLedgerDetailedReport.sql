--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('InventoryLedgerDetailedReport', True)) }}

{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date','msku'], 
    unique_key = ['date','fulfillment_center','msku', 'event_type', 'reference_id','quantity','disposition'])}}
{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['date','fulfillment_center','msku', 'event_type', 'reference_id','quantity','disposition'])}}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
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
where lower(table_name) like '%ledgerdetail%' 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}
    {% if var('brand_consolidation_flag') %}
        {% set id =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set id = var('brand_name') %}
    {% endif %}

    SELECT * except(row_num)
    From (
        select 
        '{{id}}' as Brand,
        reportstartdate,
        reportenddate,
        reportrequesttime,
        sellingpartnerid,
        marketplacename,
        marketplaceid,
        {% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
            cast(DATETIME_ADD(cast(Date as timestamp), INTERVAL {{hr}} HOUR ) as DATE) Date,
        {% else %}
        Date,
        {% endif %}
        fnsku,
        asin,
        msku,
        title,
        event_type,
        reference_id,
        quantity,
        fulfillment_center,
        disposition,
        reason,
        country,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
           DATETIME_ADD(cast(Date as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(Date as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        ROW_NUMBER() OVER (PARTITION BY Date, msku, fulfillment_center, event_type, reference_id order by _daton_batch_runtime desc) row_num
        from {{i}}    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}