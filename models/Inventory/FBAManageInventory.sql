{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'ReportstartDate', 'data_type': 'date' },
    cluster_by = ['sku'], 
    unique_key = ['ReportstartDate','sku']
)}}
{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['ReportstartDate','sku']
)}}
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
where lower(table_name) like '%fbamanageinventory' 
{% endset %}  

-- ddddd test

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

    SELECT * except(row_num)
    From (
        select '{{id}}' as Brand,
        CAST(ReportstartDate as DATE) ReportstartDate,
        ReportendDate,
        ReportRequestTime,
        sellingPartnerId,
        marketplaceName,
        marketplaceId,
        sku,
        fnsku,
        asin,
        product_name,
        condition,
        your_price,
        mfn_listing_exists,
        mfn_fulfillable_quantity,
        afn_listing_exists,
        afn_warehouse_quantity,
        afn_fulfillable_quantity,
        afn_fulfillable_quantity_local,
        afn_fulfillable_quantity_remote,
        afn_unsellable_quantity,
        afn_reserved_quantity,
        afn_total_quantity,
        per_unit_volume,
        afn_inbound_working_quantity,
        afn_inbound_shipped_quantity,
        afn_inbound_receiving_quantity,
        afn_researching_quantity,
        afn_reserved_future_supply,
        afn_future_supply_buyable,
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
        
        DENSE_RANK() OVER (PARTITION BY date(ReportstartDate),
        sku order by _daton_batch_runtime desc) row_num
        from {{i}}    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}
    
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}