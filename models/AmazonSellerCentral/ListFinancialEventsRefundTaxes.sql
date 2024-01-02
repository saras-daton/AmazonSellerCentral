{% if var('ListFinancialEvents') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}
 

{% set result =set_table_name("ListFinancialEvents_tbl_ptrn","ListFinancialEvents_tbl_exclude_ptrn") %}

select *, row_number() over (partition by date(RefundEventlist_PostedDate), RefundEventlist_MarketplaceName, RefundEventlist_AmazonOrderId order by _daton_batch_runtime, TaxesWithheld_ChargeType, ShipmentItemAdjustmentList_QuantityShipped) as _seq_id
from (
{% for i in result %}

        select 
        a.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.ChargeAmount_CurrencyCode') }},
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,        {% if target.type=='snowflake' %} 
            {{ timezone_conversion("RefundEventlist.value:PostedDate") }} as RefundEventlist_PostedDate,
        {% else %}
            {{ timezone_conversion("RefundEventlist.PostedDate") }} as RefundEventlist_PostedDate,
        {% endif %}
        {{extract_nested_value("RefundEventlist","AmazonOrderId","string")}} as RefundEventlist_AmazonOrderId,
        {{extract_nested_value("RefundEventlist","MarketplaceName","string")}} as RefundEventlist_MarketplaceName,
        {{extract_nested_value("ShipmentItemAdjustmentList","SellerSKU","string")}} as ShipmentItemAdjustmentList_SellerSKU,
        {{extract_nested_value("ShipmentItemAdjustmentList","QuantityShipped","integer")}} as ShipmentItemAdjustmentList_QuantityShipped,
        {{extract_nested_value("TaxesWithheld","ChargeType","string")}} as TaxesWithheld_ChargeType,
        {{extract_nested_value("ChargeAmount","CurrencyCode","string")}} as ChargeAmount_CurrencyCode,
        {{extract_nested_value("ChargeAmount","CurrencyAmount","numeric")}} as ChargeAmount_CurrencyAmount,
	   	{{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id
        from  {{i}} 
        {{unnesting("RefundEventlist")}}
        {{multi_unnesting("RefundEventlist","ShipmentItemAdjustmentList")}}
        {{multi_unnesting("ShipmentItemAdjustmentList","ItemTaxWithHeldList")}}
        {{multi_unnesting("ItemTaxWithHeldList","TaxesWithheld")}}
        {{multi_unnesting("TaxesWithheld","ChargeAmount")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListFinancialEvents_lookback') }},0) from {{ this }})
        {% endif %} 
        )a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(RefundEventlist_PostedDate) = c.date and a.ChargeAmount_CurrencyCode = c.to_currency_code
            {% endif %}
        
        qualify dense_rank() over (partition by date(RefundEventlist_PostedDate), RefundEventlist_MarketplaceName, RefundEventlist_AmazonOrderId, TaxesWithheld_ChargeType order by _daton_batch_runtime desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )
