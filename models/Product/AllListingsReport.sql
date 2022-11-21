{{config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by = ['seller_sku'],
    unique_key = 'seller_sku')}}

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
where lower(table_name) like '%alllistingsreport' 
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
        CAST(ReportstartDate as timestamp) ReportstartDate,
        CAST(ReportendDate as timestamp) ReportendDate,
        ReportRequestTime,
        sellingPartnerId,
        marketplaceName,
        marketplaceId,
        item_name,
        item_description,
        listing_id,
        seller_sku,
        price,
        quantity,
        open_date,
        image_url,
        item_is_marketplace,
        product_id_type,
        zshop_shipping_fee,
        item_note,
        item_condition,
        zshop_category1,
        zshop_browse_path,
        zshop_storefront_feature,
        asin1,
        asin2,
        asin3,
        will_ship_internationally,
        expedited_shipping,
        zshop_boldface,
        product_id,
        bid_for_featured_placement,
        add_delete,
        pending_quantity,
        fulfillment_channel,
        optional_payment_type_exclusion,
        merchant_shipping_group,
        status,
        maximum_retail_price,
        scheduled_delivery_sku_set,
        standard_price_point,
        ProductTaxCode,
        minimum_seller_allowed_price,
        maximum_seller_allowed_price,
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
        Dense_Rank() OVER (PARTITION BY seller_sku order by _daton_batch_runtime desc) row_num
	    from {{i}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}    
        ) 
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}