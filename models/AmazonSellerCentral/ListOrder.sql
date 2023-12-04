{% if var('ListOrder') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set result =set_table_name("ListOrder_tbl_ptrn","ListOrder_tbl_exclude_ptrn") %}

{% for i in result %}

       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,        {{ timezone_conversion("RequestStartDate") }} as RequestStartDate,
        {{ timezone_conversion("RequestEndDate") }} as RequestEndDate,
        sellingPartnerId,
        marketplaceName,
        AmazonOrderId,
        SellerOrderId,
        {{ timezone_conversion("PurchaseDate") }} as PurchaseDate,
        {{ timezone_conversion("LastUpdateDate") }} as LastUpdateDate,
        OrderStatus,
        FulfillmentChannel,
        SalesChannel,
        OrderChannel,
        ShipServiceLevel,
        {{extract_nested_value("OrderTotal","CurrencyCode","string")}} as OrderTotal_CurrencyCode,
        {{extract_nested_value("OrderTotal","Amount","numeric")}} as OrderTotal_Amount,
        NumberOfItemsShipped,
        NumberOfItemsUnshipped,
        {{extract_nested_value("Payment","CurrencyCode","string")}} as PaymentExecutionDetail_Payment_CurrencyCode,
        {{extract_nested_value("Payment","Amount","numeric")}} as PaymentExecutionDetail_Payment_Amount,
        {{extract_nested_value("PaymentExecutionDetail","PaymentMethod","string")}} as PaymentExecutionDetail_PaymentMethod,
        a.PaymentMethod,
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
        {{extract_nested_value("DefaultShipFromLocationAddress","name","string")}} as DefaultShipFromLocationAddress_Name,
        {{extract_nested_value("DefaultShipFromLocationAddress","AddressLine1","string")}} as DefaultShipFromLocationAddress_AddressLine1,
        {{extract_nested_value("DefaultShipFromLocationAddress","AddressLine2","string")}} as DefaultShipFromLocationAddress_AddressLine2,
        {{extract_nested_value("DefaultShipFromLocationAddress","AddressLine3","string")}} as DefaultShipFromLocationAddress_AddressLine3,
        {{extract_nested_value("DefaultShipFromLocationAddress","City","string")}} as DefaultShipFromLocationAddress_City,
        {{extract_nested_value("DefaultShipFromLocationAddress","County","string")}} as DefaultShipFromLocationAddress_County,
        {{extract_nested_value("DefaultShipFromLocationAddress","District","string")}} as DefaultShipFromLocationAddress_District,
        {{extract_nested_value("DefaultShipFromLocationAddress","StateOrRegion","string")}} as DefaultShipFromLocationAddress_StateOrRegion,
        {{extract_nested_value("DefaultShipFromLocationAddress","Municipality","string")}} as DefaultShipFromLocationAddress_Municipality,
        {{extract_nested_value("DefaultShipFromLocationAddress","PostalCode","string")}} as DefaultShipFromLocationAddress_PostalCode,
        {{extract_nested_value("DefaultShipFromLocationAddress","CountryCode","string")}} as DefaultShipFromLocationAddress_CountryCode,
        {{extract_nested_value("DefaultShipFromLocationAddress","Phone","string")}} as DefaultShipFromLocationAddress_Phone,
        {{extract_nested_value("DefaultShipFromLocationAddress","AddressType","string")}} as DefaultShipFromLocationAddress_AddressType,
        BuyerInvoicePreference,
        {{extract_nested_value("BuyerTaxInformation","BuyerLegalCompanyName","string")}} as BuyerTaxInformation_BuyerLegalCompanyName,
        {{extract_nested_value("BuyerTaxInformation","BuyerBusinessAddress","string")}} as BuyerTaxInformation_BuyerBusinessAddress,
        {{extract_nested_value("BuyerTaxInformation","BuyerTaxRegistrationId","string")}} as BuyerTaxInformation_BuyerTaxRegistrationId,
        {{extract_nested_value("BuyerTaxInformation","BuyerTaxOffice","string")}} as BuyerTaxInformation_BuyerTaxOffice,

        {{extract_nested_value("FulfillmentInstruction","FulfillmentSupplySourceId","string")}} as FulfillmentInstruction_FulfillmentSupplySourceId,

        IsISPU,
        {{extract_nested_value("TaxClassifications","Name","string")}} as MarketplaceTaxInfo_TaxClassifications_Name,
        {{extract_nested_value("TaxClassifications","Value","string")}} as MarketplaceTaxInfo_TaxClassifications_Value,

        {{extract_nested_value("ShippingAddress","name","string")}} as ShippingAddress_Name,
        {{extract_nested_value("ShippingAddress","AddressLine1","string")}} as ShippingAddress_AddressLine1,
        {{extract_nested_value("ShippingAddress","AddressLine2","string")}} as ShippingAddress_AddressLine2,
        {{extract_nested_value("ShippingAddress","AddressLine3","string")}} as ShippingAddress_AddressLine3,
        {{extract_nested_value("ShippingAddress","City","string")}} as ShippingAddress_City,
        {{extract_nested_value("ShippingAddress","County","string")}} as ShippingAddress_County,
        {{extract_nested_value("ShippingAddress","District","string")}} as ShippingAddress_District,
        {{extract_nested_value("ShippingAddress","StateOrRegion","string")}} as ShippingAddress_StateOrRegion,
        {{extract_nested_value("ShippingAddress","Municipality","string")}} as ShippingAddress_Municipality,
        {{extract_nested_value("ShippingAddress","PostalCode","string")}} as ShippingAddress_PostalCode,
        {{extract_nested_value("ShippingAddress","CountryCode","string")}} as ShippingAddress_CountryCode,
        {{extract_nested_value("ShippingAddress","Phone","string")}} as ShippingAddress_Phone,
        {{extract_nested_value("ShippingAddress","AddressType","string")}} as ShippingAddress_AddressType,
        
        SellerDisplayName,
        {{extract_nested_value("BuyerInfo","BuyerEmail","string")}} as BuyerInfo_BuyerEmail,
        {{extract_nested_value("BuyerInfo","BuyerName","string")}} as BuyerInfo_BuyerName,
        {{extract_nested_value("BuyerInfo","BuyerCounty","string")}} as BuyerInfo_BuyerCounty,
        {{extract_nested_value("BuyerInfo","PurchaseOrderNumber","string")}} as BuyerInfo_PurchaseOrderNumber,
        
        {{extract_nested_value("AutomatedShippingSettings","HasAutomatedShippingSettings","string")}} as AutomatedShippingSettings_HasAutomatedShippingSettings,
        {{extract_nested_value("AutomatedShippingSettings","AutomatedCarrier","string")}} as AutomatedShippingSettings_AutomatedCarrier,
        {{extract_nested_value("AutomatedShippingSettings","AutomatedShipMethod","string")}} as AutomatedShippingSettings_AutomatedShipMethod,
        
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("OrderTotal")}}
        {{unnesting("PaymentExecutionDetail")}}
        {{multi_unnesting("PaymentExecutionDetail","Payment")}}            
        {{unnesting("DefaultShipFromLocationAddress")}}
        {{unnesting("BuyerTaxInformation")}}
        {{unnesting("FulfillmentInstruction")}}
        {{unnesting("MarketplaceTaxInfo")}}
        {{multi_unnesting("MarketplaceTaxInfo","TaxClassifications")}}            
        {{unnesting("ShippingAddress")}}
        {{unnesting("BuyerInfo")}}
        {{unnesting("AutomatedShippingSettings")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListOrder_lookback') }},0) from {{ this }})
        {% endif %} 
    qualify row_number() over (partition by date(PurchaseDate), AmazonOrderId, marketplaceName order by _daton_batch_runtime desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}