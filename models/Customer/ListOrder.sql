{% if var('table_partition_flag') %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by = { 'field': 'PurchaseDate', 'data_type': 'date' },
    cluster_by = ['amazonorderid'],
    unique_key = ['PurchaseDate','amazonorderid'])}}
{% else %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['PurchaseDate','amazonorderid'])}}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT MAX(_daton_batch_runtime) - 2592000000 FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%listorder' 
and lower(table_name) not like '%mws%'
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{% for i in results_list %}

    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}
    SELECT * except(row_num),
    From (
        select
        RequestStartDate,
        RequestEndDate,
        sellingPartnerId,
        marketplaceName,
        AmazonOrderId,
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
        BuyerInfo.BuyerEmail,
        BuyerInfo.BuyerName,
        BuyerInfo.BuyerCounty,
        BuyerInfo.BuyerTaxInfo,
        BuyerInfo.PurchaseOrderNumber,
        AutomatedShippingSettings,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
            DATETIME_ADD(cast(LastUpdateDate as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
            CAST(LastUpdateDate as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        Dense_Rank() OVER (PARTITION BY Date(PurchaseDate), amazonorderid order by _daton_batch_runtime desc) row_num
	    from {{i}} 
                cross join unnest(BuyerInfo) BuyerInfo
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE _daton_batch_runtime  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
