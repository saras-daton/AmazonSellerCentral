{% if var('ListOrder') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}

    {% set table_name_query %}
    {{set_table_name('%listorder')}}    
    {% endset %}  

    {% set results = run_query(table_name_query) %}
    {% if execute %}
        {# Return the first column #}
        {% set results_list = results.columns[0].values() %}
        {% set tables_lowercase_list = results.columns[1].values() %}
    {% else %}
        {% set results_list = [] %}
        {% set tables_lowercase_list = [] %}
    {% endif %}


    {% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
         {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
         {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}
    
    select * from (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="RequestStartDate") }} as {{ dbt.type_timestamp() }}) as RequestStartDate,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="RequestEndDate") }} as {{ dbt.type_timestamp() }}) as RequestEndDate,
        sellingPartnerId,
        marketplaceName,
        coalesce(AmazonOrderId,'N/A') as AmazonOrderId,
        SellerOrderId,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="PurchaseDate") }} as {{ dbt.type_timestamp() }}) as PurchaseDate,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="LastUpdateDate") }} as {{ dbt.type_timestamp() }}) as LastUpdateDate,
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
        {% if target.type == 'snowflake' %} 
        BuyerInfo.value:BuyerEmail :: varchar as BuyerEmail,
        BuyerInfo.value:BuyerName :: varchar as BuyerName,
        BuyerInfo.value:BuyerCounty :: varchar as BuyerCounty,
        BuyerInfo.value:BuyerTaxInfo :: varchar as BuyerTaxInfo,
        BuyerInfo.value:PurchaseOrderNumber :: varchar as PurchaseOrderNumber,
        {% else %}
        BuyerInfo.BuyerEmail,
        BuyerInfo.BuyerName,
        BuyerInfo.BuyerCounty,
        BuyerInfo.BuyerTaxInfo,
        BuyerInfo.PurchaseOrderNumber,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} 
        {{unnesting("BuyerInfo")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    )
    qualify row_number() over (partition by date(PurchaseDate), AmazonOrderId, marketplaceName order by _daton_batch_runtime desc) = 1

    {% if not loop.last %} union all {% endif %}
    {% endfor %}