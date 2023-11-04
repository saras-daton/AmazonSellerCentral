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

    select *, row_number() over (partition by date(RequestStartDate), marketplacename, ServiceFeeEventList_FeeReason, FeeList_FeeType, ServiceFeeEventList_SellerSKU, ServiceFeeEventList_FeeDescription order by _daton_batch_runtime, _daton_batch_id) as _seq_id
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
            '{{brand|replace("`","")}}' as brand,
            '{{store|replace("`","")}}' as store,
            a.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.FeeAmount_CurrencyCode else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                a.FeeAmount_CurrencyCode as exchange_currency_code, 
            {% endif %}
	   		a._daton_user_id,
            a._daton_batch_runtime,
            a._daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
            from (
            select
            {{ timezone_conversion("RequestStartDate") }} as RequestStartDate,
            {{ timezone_conversion("RequestEndDate") }} as RequestEndDate,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            coalesce({{extract_nested_value("ServiceFeeEventList","AmazonOrderId","string")}},'N/A') as ServiceFeeEventList_AmazonOrderId,
            coalesce({{extract_nested_value("ServiceFeeEventList","FeeReason","string")}},'N/A') as ServiceFeeEventList_FeeReason,
            coalesce({{extract_nested_value("FeeList","FeeType","string")}},'N/A') as FeeList_FeeType,
            {{extract_nested_value("FeeAmount","CurrencyCode","string")}} as FeeAmount_CurrencyCode,
            {{extract_nested_value("FeeAmount","CurrencyAmount","numeric")}} as FeeAmount_CurrencyAmount,
            coalesce({{extract_nested_value("ServiceFeeEventList","SellerSKU","string")}},'N/A') as ServiceFeeEventList_SellerSKU,
            coalesce({{extract_nested_value("ServiceFeeEventList","FnSKU","string")}},'N/A') as ServiceFeeEventList_FnSKU,
            {{extract_nested_value("ServiceFeeEventList","FeeDescription","string")}} as ServiceFeeEventList_FeeDescription,
            {{extract_nested_value("ServiceFeeEventList","ASIN","string")}} as ServiceFeeEventList_ASIN,
            {{daton_user_id()}} as _daton_user_id,
       		{{daton_batch_runtime()}} as _daton_batch_runtime,
        	{{daton_batch_id()}} as _daton_batch_id
            from {{i}} 
                {{unnesting("ServiceFeeEventList")}}
                {{multi_unnesting("ServiceFeeEventList","FeeList")}}
                {{multi_unnesting("FeeList","FeeAmount")}}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListFinancialEvents_lookback') }},0) from {{ this }})
            {% endif %} 
            ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.RequestStartDate) = c.date and a.FeeAmount_CurrencyCode = c.to_currency_code
            {% endif %}
        )
        qualify dense_rank() over (partition by date(RequestStartDate), marketplaceId, ServiceFeeEventList_FeeReason, FeeList_FeeType, ServiceFeeEventList_SellerSKU, ServiceFeeEventList_FeeDescription order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )
