--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('FBAReturnsReport', True)) }}

{% if var('table_partition_flag') %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by = { 'field': 'return_date', 'data_type': 'date' },
    cluster_by = ['asin','sku'],
    unique_key = ['return_date','asin','sku','order_id','fnsku','license_plate_number'])}}
{% else %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['return_date','asin','sku','order_id','fnsku','license_plate_number', '_seq_id'])}}
{% endif %}

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
where lower(table_name) like '%fbareturnsreport' 
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
    {% if var('brand_consolidation_flag') %}
        {% set id =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set id = var('brand_name') %}
    {% endif %}

    
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date(return_date), asin, sku, order_id, fnsku, license_plate_number order by _daton_batch_runtime desc) as _seq_id 
    from (
        select * except(row_num)
        From (
            select '{{id}}' as brand,
            CAST(ReportstartDate as timestamp) ReportstartDate,
            CAST(ReportendDate as timestamp) ReportendDate,
            CAST(ReportRequestTime as timestamp) ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            cast(return_date as DATE) as return_date,
            order_id,
            sku,
            asin,
            fnsku,
            product_name,
            quantity,
            fulfillment_center_id,
            detailed_disposition,
            reason,
            status,
            license_plate_number,
            customer_comments,
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
            Dense_Rank() OVER (PARTITION BY date(return_date), asin, sku, order_id, fnsku, license_plate_number order by _daton_batch_runtime desc) row_num
            from {{i}}    
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE _daton_batch_runtime  >= {{max_loaded}}
                {% endif %}
            ) where row_num = 1
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}


