{% if var('ListFinancialEventsRefundPromotions') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
 -- depends_on: {{ ref('ExchangeRates') }}  
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
    
    select *, row_number() over (partition by date(RefundEventlist_PostedDate), RefundEventlist_MarketplaceName, RefundEventlist_AmazonOrderId order by _daton_batch_runtime, PromotionList_PromotionType, ShipmentItemAdjustmentList_QuantityShipped) as _seq_id
    from (
    {% set table_name_query %}
    {{set_table_name('%listfinancialevents')}}    
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

        select * from (
        select 
        a.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.PromotionAmount_CurrencyCode else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.PromotionAmount_CurrencyCode as exchange_currency_code, 
        {% endif %}
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        '{{brand}}' as Brand,
        '{{store}}' as store,
        {% if target.type=='snowflake' %} 
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="RefundEventlist.value:PostedDate") }} as {{ dbt.type_timestamp() }}) as RefundEventlist_PostedDate,
            RefundEventlist.value:AmazonOrderId :: varchar as RefundEventlist_AmazonOrderId,
            RefundEventlist.value:MarketplaceName :: varchar as RefundEventlist_MarketplaceName,
            ShipmentItemAdjustmentList.value:SellerSKU :: varchar as ShipmentItemAdjustmentList_SellerSKU,
            ShipmentItemAdjustmentList.value:QuantityShipped :: integer as ShipmentItemAdjustmentList_QuantityShipped,
            PromotionList.value:PromotionType :: varchar as PromotionList_PromotionType,
            PromotionAmount.value:CurrencyCode :: varchar as PromotionAmount_CurrencyCode,
            PromotionAmount.value:CurrencyAmount :: float as PromotionAmount_CurrencyAmount,
        {% else %}
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(RefundEventlist.PostedDate as timestamp)") }} as {{ dbt.type_timestamp() }}) as RefundEventlist_PostedDate,
            coalesce(RefundEventlist.amazonorderid,'N/A') as RefundEventlist_AmazonOrderId,
            coalesce(RefundEventlist.marketplacename,'N/A') as RefundEventlist_MarketplaceName,
            coalesce(ShipmentItemAdjustmentList.SellerSKU,'N/A') as ShipmentItemAdjustmentList_SellerSKU,
            cast(ShipmentItemAdjustmentList.QuantityShipped as integer) as ShipmentItemAdjustmentList_QuantityShipped,
            coalesce(PromotionList.PromotionType,'N/A') as PromotionList_PromotionType,
            PromotionAmount.CurrencyCode as PromotionAmount_CurrencyCode,
            PromotionAmount.CurrencyAmount as PromotionAmount_CurrencyAmount,
        {% endif %}
	   	{{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id
        from  {{i}} 
            {{unnesting("RefundEventlist")}}
            {{multi_unnesting("RefundEventlist","ShipmentItemAdjustmentList")}}
            {{multi_unnesting("ShipmentItemAdjustmentList","PromotionList")}}
            {{multi_unnesting("PromotionList","PromotionAmount")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(RefundEventlist_PostedDate) = c.date and a.PromotionAmount_CurrencyCode = c.to_currency_code
            {% endif %}
        )
        qualify dense_rank() over (partition by date(RefundEventlist_PostedDate), RefundEventlist_MarketplaceName, RefundEventlist_AmazonOrderId, PromotionList_PromotionType order by _daton_batch_runtime desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )
    