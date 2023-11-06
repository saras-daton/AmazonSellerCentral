{% if var('SalesAndTrafficReportByChildASIN') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('SalesAndTrafficReportByChildASIN_tbl_ptrn'),
exclude=var('SalesAndTrafficReportByChildASIN_tbl_exclude_ptrn'),
database=var('raw_database')) %}

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

        select 
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
        {{ timezone_conversion("ReportendDate") }} as ReportendDate,
        {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            a.date,
            coalesce(parentAsin,'N/A') as parentAsin,
            coalesce(childAsin,'N/A') as childAsin,
            unitsOrdered,
            unitsOrderedB2B,
            orderedProductSales_amount,
            orderedProductSales_currencyCode,
            orderedProductSalesB2B_amount,
            orderedProductSalesB2B_currencyCode,
            totalOrderItems,
            totalOrderItemsB2B,
            browserSessions,
            mobileAppSessions,
            sessions,
            browserSessionPercentage,
            mobileAppSessionPercentage,
            sessionPercentage,
            browserPageViews,
            mobileAppPageViews,
            pageViews,
            browserPageViewsPercentage,
            mobileAppPageViewsPercentage,
            pageViewsPercentage,
            buyBoxPercentage,
            unitSessionPercentage,
            unitSessionPercentageB2B,
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.orderedProductSales_currencyCode else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                a.orderedProductSales_currencyCode as exchange_currency_code, 
            {% endif %} 
	   	    a.{{daton_user_id()}} as _daton_user_id,
            a.{{daton_batch_runtime()}} as _daton_batch_runtime,
            a.{{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
            from {{i}} a 
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and a.orderedProductSales_currencyCode = c.to_currency_code   
            {% endif %}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('SalesAndTrafficReportByChildASIN_lookback') }},0) from {{ this }})
            {% endif %} 

            qualify row_number() over (partition by '{{brand}}', a.date, parentAsin, childASIN, marketplaceId  order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}