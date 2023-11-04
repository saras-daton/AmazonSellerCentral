{% if var('FBAManageInventory') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('FBAManageInventory_tbl_ptrn'),
exclude=var('FBAManageInventory_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    select 
    '{{brand|replace("`","")}}' as brand,
    '{{store|replace("`","")}}' as store,
    {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
    {{ timezone_conversion("ReportendDate") }} as ReportendDate,
    {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
    sellingPartnerId,
    marketplaceName,
    marketplaceId,
    coalesce(sku,'N/A') as sku,
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
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}   
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAManageInventory_lookback') }},0) from {{ this }})
        {% endif %} 
    qualify dense_rank() over (partition by date(ReportstartDate),sku, marketplaceId order by {{daton_batch_runtime()}} desc) = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}