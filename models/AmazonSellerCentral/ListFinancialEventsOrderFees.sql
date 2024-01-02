{% if var('ListFinancialEvents') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}
 


{% set result =set_table_name("ListFinancialEvents_tbl_ptrn","ListFinancialEvents_tbl_exclude_ptrn") %}


select *, row_number() over (partition by date(ShipmentEventlist_PostedDate), ShipmentEventlist_MarketplaceName, ShipmentEventlist_AmazonOrderId order by _daton_batch_runtime, ItemFeeList_FeeType, ShipmentItemlist_QuantityShipped) as _seq_id 
from (

{% for i in result %}

       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,           a.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
            {{ currency_conversion('c.value', 'c.from_currency_code', 'a.FeeAmount_CurrencyCode') }},
	   		a._daton_user_id,
            a._daton_batch_runtime,
            a._daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
            from (
            select
            {% if target.type=='snowflake' %} 
                {{ timezone_conversion("ShipmentEventlist.value:PostedDate") }} as ShipmentEventlist_PostedDate,
            {% else %}
                {{ timezone_conversion("ShipmentEventlist.PostedDate") }} as ShipmentEventlist_PostedDate,
            {% endif %}
            {{extract_nested_value("ShipmentEventlist","AmazonOrderId","string")}} as ShipmentEventlist_AmazonOrderId,
            {{extract_nested_value("ShipmentEventlist","MarketplaceName","string")}} as ShipmentEventlist_MarketplaceName,
            {{extract_nested_value("ShipmentItemList","SellerSKU","string")}} as ShipmentItemlist_SellerSKU,
            {{extract_nested_value("ShipmentItemList","QuantityShipped","integer")}} as ShipmentItemlist_QuantityShipped,
            {{extract_nested_value("ItemFeeList","FeeType","string")}} as ItemFeeList_FeeType,
            {{extract_nested_value("FeeAmount","CurrencyCode","string")}} as FeeAmount_CurrencyCode,
            {{extract_nested_value("FeeAmount","CurrencyAmount","numeric")}} as FeeAmount_CurrencyAmount,
            {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id
            from {{i}} 
                {{unnesting("ShipmentEventlist")}}
                {{multi_unnesting("ShipmentEventlist","ShipmentItemList")}}
                {{multi_unnesting("ShipmentItemList","ItemFeeList")}}
                {{multi_unnesting("ItemFeeList","FeeAmount")}}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListFinancialEvents_lookback') }},0) from {{ this }})
            {% endif %}  
            ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.ShipmentEventlist_PostedDate) = c.date and a.FeeAmount_CurrencyCode = c.to_currency_code
            {% endif %}

        qualify dense_rank() over (partition by date(ShipmentEventlist_PostedDate), ShipmentEventlist_MarketplaceName, ShipmentEventlist_AmazonOrderId, ItemFeeList_FeeType order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )
