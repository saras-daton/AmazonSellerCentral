{% if var('FBAManageInventoryHealthReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %} 

{% set result =set_table_name("FBAManageInventoryHealthReport_tbl_ptrn","FBAManageInventoryHealthReport_tbl_exclude_ptrn") %}

{% for i in result %}

    select 
        '{{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }}' as brand,
        '{{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }}' as store,
    {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
    {{ timezone_conversion("ReportendDate") }} as ReportendDate,
    {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
    sellingPartnerId,
    marketplaceName,
    marketplaceId,
    cast(snapshot_date as DATE) snapshot_date,
    sku,
    fnsku,
    asin,
    product_name,
    condition,
    available,
    pending_removal_quantity,
    inv_age_0_to_90_days,
    inv_age_91_to_180_days,
    inv_age_181_to_270_days,
    inv_age_271_to_365_days,
    inv_age_365_plus_days,
    currency,
    qty_to_be_charged_ltsf_6_mo,
    projected_ltsf_6_mo,
    qty_to_be_charged_ltsf_12_mo,
    estimated_ltsf_next_charge,
    units_shipped_t7,
    units_shipped_t30,
    units_shipped_t60,
    units_shipped_t90,
    alert,
    your_price,
    sales_price,
    lowest_price_new_plus_shipping,
    lowest_price_used,
    recommended_action,
    healthy_inventory_level,
    recommended_sales_price,
    recommended_sale_duration_days,
    recommended_removal_quantity,
    estimated_cost_savings_of_recommended_actions,
    sell_through,
    item_volume,
    volume_unit_measurement,
    storage_type,
    storage_volume,
    marketplace,
    product_group,
    sales_rank,
    days_of_supply,
    estimated_excess_quantity,
    weeks_of_cover_t30,
    weeks_of_cover_t90,
    featuredoffer_price,
    sales_shipped_last_7_days,
    sales_shipped_last_30_days,
    sales_shipped_last_60_days,
    sales_shipped_last_90_days,
    inv_age_0_to_30_days,
    inv_age_31_to_60_days,
    inv_age_61_to_90_days,
    inv_age_181_to_330_days,
    inv_age_331_to_365_days,
    estimated_storage_cost_next_month,
    inbound_quantity,
    inbound_working,
    inbound_shipped,
    inbound_received,
    no_sale_last_6_months,
    reserved_quantity,
    unfulfillable_quantity,
    quantity_to_be_charged_ais_181_210_days, 
    estimated_ais_181_210_days, 
    quantity_to_be_charged_ais_211_240_days, 
    estimated_ais_211_240_days, 
    quantity_to_be_charged_ais_241_270_days, 
    estimated_ais_241_270_days, 
    quantity_to_be_charged_ais_271_300_days, 
    estimated_ais_271_300_days, 
    quantity_to_be_charged_ais_301_330_days, 
    estimated_ais_301_330_days, 
    quantity_to_be_charged_ais_331_365_days, 
    estimated_ais_331_365_days, 
    quantity_to_be_charged_ais_365_PLUS_days, 
    estimated_ais_365_plus_days,
    {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
    {% if var('currency_conversion_flag') %}
        left join {{ref('ExchangeRates')}} c on date(a.ReportRequestTime) = c.date and a.currency = c.to_currency_code
    {% endif %}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAManageInventoryHealthReport_lookback') }},0) from {{ this }})
    {% endif %}
    qualify dense_rank() over (partition by snapshot_date, asin, sku, marketplaceId order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
