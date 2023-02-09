
    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
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
    {% else %}
    {% set results_list = [] %}
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
    
     select * {{exclude()}} (row_num)from (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY Date(PurchaseDate), amazonorderid order by {{daton_batch_runtime()}} desc) row_num
            From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
             RequestStartDate,
            RequestEndDate,
            sellingPartnerId,
            marketplaceName,
            coalesce(AmazonOrderId,'') as AmazonOrderId,
            SellerOrderId,
            {% if var('timezone_conversion_flag') %}
                cast(DATETIME_ADD(PurchaseDate, INTERVAL {{hr}} HOUR ) as DATE) PurchaseDate,
                DATETIME_ADD(cast(LastUpdateDate as timestamp), INTERVAL {{hr}} HOUR ) LastUpdateDate,  
            {% else %}
                cast(PurchaseDate as DATE) PurchaseDate,
                CAST(LastUpdateDate as timestamp) LastUpdateDate,
            {% endif %}
            OrderStatus,
            FulfillmentChannel,
            SalesChannel,
            OrderChannel,
            ShipServiceLevel,
            OrderTotal,
            NumberOfItemsShipped,
            NumberOfItemsUnshipped,
            PaymentExecutionDetail,
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
            DefaultShipFromLocationAddress,
            BuyerInvoicePreference,
            BuyerTaxInformation,
            FulfillmentInstruction,
            IsISPU,
            MarketplaceTaxInfo,
            SellerDisplayName,
            ShippingAddress,
            {% if target.type == 'snowflake' %} 
            BUYERINFO.VALUE:BuyerEmail :: VARCHAR as BuyerEmail,
            BUYERINFO.VALUE:BuyerName :: VARCHAR as BuyerName,
            BUYERINFO.VALUE:BuyerCounty :: VARCHAR as BuyerCounty,
            BUYERINFO.VALUE:BuyerTaxInfo :: VARCHAR as BuyerTaxInfo,
            BuyerInfo.VALUE:PurchaseOrderNumber :: VARCHAR as PurchaseOrderNumber,
            {% else %}
            BuyerInfo.BuyerEmail,
            BuyerInfo.BuyerName,
            BuyerInfo.BuyerCounty,
            BuyerInfo.BuyerTaxInfo,
            BuyerInfo.PurchaseOrderNumber,
            {% endif %}
            AutomatedShippingSettings,
	   	    {{daton_user_id()}},
       	    {{daton_batch_runtime()}},
            {{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(LastUpdateDate as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id
            {% else %}
                cast(LastUpdateDate as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id
            {% endif %}
            FROM {{i}} 
            {{unnesting("BUYERINFO")}}
               {% if is_incremental() %}
               {# /* -- this filter will only be applied on an incremental run */ #}
               WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
               {% endif %}


    ) unnested_BUYERINFO
    ) final
    where row_num = 1


    {% if not loop.last %} union all {% endif %}
    {% endfor %}