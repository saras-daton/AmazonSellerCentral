{% if var('FBALongTermStorageFeeChargesReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("FBALongTermStorageFeeChargesReport_tbl_ptrn","FBALongTermStorageFeeChargesReport_tbl_exclude_ptrn") %}

{% for i in result %}

    select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,        {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
        {{ timezone_conversion("ReportendDate") }} as ReportendDate,
        {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
        sellingPartnerId, 
        marketplaceName, 
        marketplaceId, 
        {{ timezone_conversion("snapshot_date") }} as snapshot_date, 
        sku, 
        fnsku, 
        asin, 
        product_name, 
        condition, 
        qty_charged_long_time_range_long_term_storage_fee, 
        per_unit_volume, 
        currency, 
        long_time_range_long_term_storage_fee, 
        qty_charged_short_time_range_long_term_storage_fee, 
        short_time_range_long_term_storage_fee, 
        volume_unit, 
        country, 
        enrolled_in_small_and_light, 
        qty_charged, 
        amount_charged, 
        surcharge_age_tier, 
        rate_surcharge,
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
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBALongTermStorageFeeChargesReport_lookback') }},0) from {{ this }})
        {% endif %}  
    
        qualify dense_rank() over (partition by snapshot_date,sku, asin order by {{daton_batch_runtime()}} desc ) = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}