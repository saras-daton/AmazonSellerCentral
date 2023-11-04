{% if var('ListFinancialEvents') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}
 

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('ListFinancialEvents_tbl_ptrn'),
exclude=var('ListFinancialEvents_tbl_exclude_ptrn'),
database=var('raw_database')) %}

select *, row_number() over (partition by date(RefundEventlist_PostedDate), RefundEventlist_MarketplaceName, RefundEventlist_AmazonOrderId order by _daton_batch_runtime, ItemChargeAdjustmentList_ChargeType, ShipmentItemAdjustmentList_QuantityShipped) as _seq_id
from (
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

        select * from (
        select 
        a.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.ChargeAmount_CurrencyCode else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.ChargeAmount_CurrencyCode as exchange_currency_code, 
        {% endif %}
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (select
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        {% if target.type=='snowflake' %} 
            {{ timezone_conversion("RefundEventlist.value:PostedDate") }} as RefundEventlist_PostedDate,
        {% else %}
            {{ timezone_conversion("RefundEventlist.PostedDate") }} as RefundEventlist_PostedDate,
        {% endif %}
        coalesce({{extract_nested_value("RefundEventlist","AmazonOrderId","string")}},'N/A') as RefundEventlist_AmazonOrderId,
        coalesce({{extract_nested_value("RefundEventlist","MarketplaceName","string")}},'N/A') as RefundEventlist_MarketplaceName,
        coalesce({{extract_nested_value("ShipmentItemAdjustmentList","SellerSKU","string")}},'N/A') as ShipmentItemAdjustmentList_SellerSKU,
        {{extract_nested_value("ShipmentItemAdjustmentList","QuantityShipped","integer")}} as ShipmentItemAdjustmentList_QuantityShipped,
        coalesce({{extract_nested_value("ItemChargeAdjustmentList","ChargeType","string")}},'N/A') as ItemChargeAdjustmentList_ChargeType,
        {{extract_nested_value("ChargeAmount","CurrencyCode","string")}} as ChargeAmount_CurrencyCode,
        {{extract_nested_value("ChargeAmount","CurrencyAmount","numeric")}} as ChargeAmount_CurrencyAmount,
	   	{{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id
        from  {{i}} 
        {{unnesting("RefundEventlist")}}
        {{multi_unnesting("RefundEventlist","ShipmentItemAdjustmentList")}}
        {{multi_unnesting("ShipmentItemAdjustmentList","ItemChargeAdjustmentList")}}
        {{multi_unnesting("ItemChargeAdjustmentList","ChargeAmount")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListFinancialEvents_lookback') }},0) from {{ this }})
        {% endif %} 
        ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(RefundEventlist_PostedDate) = c.date and a.ChargeAmount_CurrencyCode = c.to_currency_code
            {% endif %}
        )
        qualify dense_rank() over (partition by date(RefundEventlist_PostedDate), RefundEventlist_MarketplaceName, RefundEventlist_AmazonOrderId, ItemChargeAdjustmentList_ChargeType order by _daton_batch_runtime desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )
    