{% if var('SalesAndTrafficReportByChildASIN') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("SalesAndTrafficReportByChildASIN_tbl_ptrn","SalesAndTrafficReportByChildASIN_tbl_exclude_ptrn") %}

{% for i in result %}

       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,            {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
            {{ timezone_conversion("ReportendDate") }} as ReportendDate,
            {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            a.date,
            parentAsin,
            childAsin,
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
            {{ currency_conversion('c.value', 'c.from_currency_code', 'a.orderedProductSales_currencyCode') }},
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

            qualify row_number() over (partition by brand, a.date, parentAsin, childASIN, marketplaceId  order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}