{% if var('ListFinancialEventsServiceFees') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
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

    select *, row_number() over (partition by date(RequestStartDate), marketplacename, ServiceFeeEventList_FeeReason, FeeList_FeeType, ServiceFeeEventList_SellerSKU, ServiceFeeEventList_FeeDescription order by _daton_batch_runtime, _daton_batch_id) as _seq_id
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
            '{{brand}}' as brand,
            '{{store}}' as store,
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
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="RequestStartDate") }} as {{ dbt.type_timestamp() }}) as RequestStartDate,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="RequestEndDate") }} as {{ dbt.type_timestamp() }}) as RequestEndDate,
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
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
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
