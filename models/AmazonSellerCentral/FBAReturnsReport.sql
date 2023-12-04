{% if var('FBAReturnsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}



{% set result =set_table_name("FBAReturnsReport_tbl_ptrn","FBAReturnsReport_tbl_exclude_ptrn") %}

{% for i in result %}

    
    select *, row_number() over (partition by date(return_date), asin, sku, order_id, fnsku, license_plate_number, fulfillment_center_id order by _daton_batch_runtime desc) as _seq_id 
    from (
              select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,        {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
        {{ timezone_conversion("ReportendDate") }} as ReportendDate,
        {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
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
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}    
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAReturnsReport_lookback') }},0) from {{ this }})
            {% endif %}
        qualify dense_Rank() over (partition by date(return_date), asin, sku, order_id, fnsku, license_plate_number, fulfillment_center_id, marketplaceId order by {{daton_batch_runtime()}} desc) = 1
        ) 
    {% if not loop.last %} union all {% endif %}
{% endfor %}