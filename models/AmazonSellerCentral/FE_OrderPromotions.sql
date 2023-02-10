 -- depends_on: {{ ref('ExchangeRates') }} 

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

   with unnested_shipmenteventlist as (
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

    SELECT * FROM (
        select 
            '{{id}}' as brand,
            '{{store}}' as store,
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.CurrencyCode  else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                cast(null as string) as exchange_currency_code, 
            {% endif %}
            a.* from (
            select 
            'Promotion' as AmountType,
            'Order' as TransactionType,
            {% if target.type=='snowflake' %} 
            ShipmentEventlist.VALUE:PostedDate :: DATE as posteddate,
            ShipmentEventlist.VALUE:AmazonOrderId :: varchar as Amazonorderid,
            ShipmentEventlist.VALUE:MarketplaceName :: varchar as marketplacename,
            ShipmentItemList.VALUE:SellerSKU :: varchar as SellerSKU,
            ShipmentItemList.VALUE:QuantityShipped :: FLOAT as QuantityShipped,
            PromotionList.value:PromotionType :: varchar as PromotionType ,
            PromotionAmount.value:CurrencyCode::varchar as CurrencyCode,
            PromotionAmount.value:CurrencyAmount::FLOAT as CurrencyAmount,
            {% else %}
            date(ShipmentEventlist.posteddate) as posteddate,
            coalesce(ShipmentEventlist.amazonorderid,'') as amazonorderid,
            coalesce(ShipmentEventlist.Marketplacename,'') as marketplacename,
            ShipmentItemList.sellerSKU as sellerSKU,
            ShipmentItemList.quantityshipped as quantityshipped,
            coalesce(PromotionList.PromotionType,'') as PromotionType,
            PromotionAmount.CurrencyCode as CurrencyCode,
            PromotionAmount.CurrencyAmount as CurrencyAmount,
            {% endif %}
	   		{{daton_user_id()}},
       		{{daton_batch_runtime()}},
        	{{daton_batch_id()}},
            current_timestamp() as last_updated,
            null as run_id
            FROM {{i}} 
            {{unnesting("ShipmentEventlist")}}
            {{multi_unnesting("ShipmentEventlist","ShipmentItemList")}}
            {{multi_unnesting("ShipmentItemList","PromotionList")}}
            {{multi_unnesting("PromotionList","PromotionAmount")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            ) a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(posteddate) = c.date and a.CurrencyCode = c.to_currency_code
            {% endif %}
        )
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )

    select *, ROW_NUMBER() OVER (PARTITION BY posteddate, marketplacename, amazonorderid order by {{daton_batch_runtime()}}, PromotionType, quantityshipped) _seq_id
    from (
        select * {{exclude()}} (rank)from (
            select *,
            DENSE_RANK() OVER (PARTITION BY posteddate, marketplacename, amazonorderid, PromotionType order by {{daton_batch_runtime()}} desc) rank
            from unnested_shipmenteventlist
        ) where rank = 1
    )




