{% if var('ListOrder') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('ListOrder_tbl_ptrn'),
exclude=var('ListOrder_tbl_exclude_ptrn'),
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

    select *
    from (
        select 
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        {{ timezone_conversion("RequestStartDate") }} as RequestStartDate,
        {{ timezone_conversion("RequestEndDate") }} as RequestEndDate,
        sellingPartnerId,
        marketplaceName,
        coalesce(AmazonOrderId,'N/A') as AmazonOrderId,
        SellerOrderId,
        {{ timezone_conversion("PurchaseDate") }} as PurchaseDate,
        {{ timezone_conversion("LastUpdateDate") }} as LastUpdateDate,
        OrderStatus,
        FulfillmentChannel,
        SalesChannel,
        OrderChannel,
        ShipServiceLevel,
        NumberOfItemsShipped,
        NumberOfItemsUnshipped,
        PaymentMethod,
        PaymentMethodDetails,
        MarketplaceId,
        ShipmentServiceLevelCategory,
        EasyShipShipmentStatus,
        CbaDisplayableShippingLabel,
        OrderType,
        EarliestShipDate,
        LatestShipDate,
        EarliestDeliveryDate,
        LatestDeliveryDate,
        IsBusinessOrder,
        IsPrime,
        IsPremiumOrder,
        IsGlobalExpressEnabled,
        ReplacedOrderId,
        IsReplacementOrder,
        PromiseResponseDueDate,
        IsEstimatedShipDateSet,
        IsSoldByAB,
        BuyerInvoicePreference,
        IsISPU,
        SellerDisplayName,
        {{extract_nested_value("BuyerInfo","BuyerEmail","string")}} as BuyerInfo_BuyerEmail,
        {{extract_nested_value("BuyerInfo","BuyerName","string")}} as BuyerInfo_BuyerName,
        {{extract_nested_value("BuyerInfo","BuyerCounty","string")}} as BuyerInfo_BuyerCounty,
        {{extract_nested_value("BuyerInfo","PurchaseOrderNumber","string")}} as BuyerInfo_PurchaseOrderNumber,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} 
        {{unnesting("BuyerInfo")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListOrder_lookback') }},0) from {{ this }})
        {% endif %} 
    )
    qualify row_number() over (partition by date(PurchaseDate), AmazonOrderId, marketplaceName order by _daton_batch_runtime desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}