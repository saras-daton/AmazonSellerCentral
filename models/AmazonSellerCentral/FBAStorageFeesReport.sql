{% if var('FBAStorageFeesReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("FBAStorageFeesReport_tbl_ptrn","FBAStorageFeesReport_tbl_exclude_ptrn") %}

{% for i in result %}

    
       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,        {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
        {{ timezone_conversion("ReportendDate") }} as ReportendDate,
        {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
        sellingPartnerId,
        marketplaceName, 
        marketplaceId, 
        asin, 
        fnsku, 
        product_name, 
        fulfillment_center, 
        country_code, 
        longest_side, 
        median_side, 
        shortest_side, 
        measurement_units, 
        weight, 
        weight_units, 
        item_volume, 
        volume_units, 
        product_size_tier, 
        average_quantity_on_hand, 
        average_quantity_pending_removal, 
        estimated_total_item_volume, 
        month_of_charge, 
        storage_utilization_ratio, 
        storage_utilization_ratio_units, 
        base_rate, 
        utilization_surcharge_rate, 
        currency, 
        estimated_monthly_storage_fee, 
        dangerous_goods_storage_type, 
        category, 
        eligible_for_inventory_discount,  
        qualifies_for_inventory_discount,
        total_incentive_fee_amount ,
        breakdown_incentive_fee_amount, 
        average_quantity_customer_orders,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}  a 
        {% if var('currency_conversion_flag') %}
        left join {{ref('ExchangeRates')}} c on date(a.ReportRequestTime) = c.date and a.currency = c.to_currency_code
        {% endif %}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('SuppressedListingsReport_lookback') }},0) from {{ this }})
        {% endif %}  
    
        qualify dense_rank() over (partition by asin, fnsku, fulfillment_center,month_of_charge order by {{daton_batch_runtime()}} desc, {{ daton_batch_id()}} desc) = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
    