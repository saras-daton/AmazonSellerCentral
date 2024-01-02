{% if var('FBAManageInventory') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% set result =set_table_name("FBAManageInventory_tbl_ptrn","FBAManageInventory_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,    {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
    {{ timezone_conversion("ReportendDate") }} as ReportendDate,
    {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
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