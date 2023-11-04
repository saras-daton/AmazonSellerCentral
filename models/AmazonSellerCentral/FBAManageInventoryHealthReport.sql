{% if var('FBAManageInventoryHealthReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %} 

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('FBAManageInventoryHealthReport_tbl_ptrn'),
exclude=var('FBAManageInventoryHealthReport_tbl_exclude_ptrn'),
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
    cast(snapshot_date as DATE) snapshot_date,
    coalesce(sku,'N/A') as sku,
    fnsku,
    coalesce(asin,'N/A') as asin,
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
    {% if var('currency_conversion_flag') %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
    {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        a.currency as exchange_currency_code, 
    {% endif %} 
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
        where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAManageInventoryHealthReport_lookback') }},0) from {{ this }})
    {% endif %}
    qualify dense_rank() over (partition by snapshot_date, asin, sku, marketplaceId order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
