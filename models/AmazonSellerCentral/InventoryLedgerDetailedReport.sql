{% if var('InventoryLedgerDetailedReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("InventoryLedgerDetailedReport_tbl_ptrn","InventoryLedgerDetailedReport_tbl_exclude_ptrn") %}

{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,        {{ timezone_conversion("reportstartdate") }} as reportstartdate,
        {{ timezone_conversion("reportenddate") }} as reportenddate,
        {{ timezone_conversion("reportrequesttime") }} as reportrequesttime,
        sellingpartnerid,
        marketplacename,
        marketplaceid,
        cast(Date as date) as Date,
        fnsku,
        asin,
        msku,
        title,
        event_type,
        reference_id,
        quantity,
        fulfillment_center,
        disposition,
        reason,
        country,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('InventoryLedgerDetailedReport_lookback') }},0) from {{ this }})
        {% endif %}  
    qualify row_number() over (partition by Date,asin, msku, fulfillment_center, event_type, reference_id, quantity, disposition, marketplaceid order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}