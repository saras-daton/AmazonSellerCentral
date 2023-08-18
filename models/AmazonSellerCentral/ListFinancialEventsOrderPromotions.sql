{% if var('ListFinancialEventsOrderPromotions') %}
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

    select *, row_number() over (partition by date(ShipmentEventlist_PostedDate), ShipmentEventlist_MarketplaceName, ShipmentEventlist_AmazonOrderId order by _daton_batch_runtime, PromotionList_PromotionType, ShipmentItemlist_QuantityShipped) as _seq_id
    from(
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
            '{{brand}}' as brand,
            '{{store}}' as store,
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
            {% if target.type=='snowflake' %} 
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ShipmentEventlist.value:PostedDate") }} as {{ dbt.type_timestamp() }}) as ShipmentEventlist_PostedDate,
            {% else %}
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(ShipmentEventlist.PostedDate as timestamp)") }} as {{ dbt.type_timestamp() }}) as ShipmentEventlist_PostedDate,
            {% endif %}
            coalesce({{extract_nested_value("ShipmentEventlist","AmazonOrderId","string")}},'N/A') as ShipmentEventlist_AmazonOrderId,
            coalesce({{extract_nested_value("ShipmentEventlist","MarketplaceName","string")}},'N/A') as ShipmentEventlist_MarketplaceName,
            coalesce({{extract_nested_value("ShipmentItemList","SellerSKU","string")}},'N/A') as ShipmentItemlist_SellerSKU,
            {{extract_nested_value("ShipmentItemList","QuantityShipped","integer")}} as ShipmentItemlist_QuantityShipped,
            coalesce({{extract_nested_value("PromotionList","PromotionType","string")}},'N/A') as PromotionList_PromotionType,
            {{extract_nested_value("PromotionAmount","CurrencyCode","string")}} as PromotionAmount_CurrencyCode,
            {{extract_nested_value("PromotionAmount","CurrencyAmount","float")}} as PromotionAmount_CurrencyAmount,
	   		{{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id
            from {{i}} 
            {{unnesting("ShipmentEventlist")}}
            {{multi_unnesting("ShipmentEventlist","ShipmentItemList")}}
            {{multi_unnesting("ShipmentItemList","PromotionList")}}
            {{multi_unnesting("PromotionList","PromotionAmount")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            ) a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(ShipmentEventlist_PostedDate) = c.date and a.PromotionAmount_CurrencyCode = c.to_currency_code
            {% endif %}
        )
        qualify dense_rank() over (partition by date(ShipmentEventlist_PostedDate), ShipmentEventlist_MarketplaceName, ShipmentEventlist_AmazonOrderId, PromotionList_PromotionType order by {{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )
