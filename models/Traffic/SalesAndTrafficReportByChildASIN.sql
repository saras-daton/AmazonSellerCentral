-- depends_on: {{ref('ExchangeRates')}}

--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('SalesAndTrafficReportByChildASIN', True)) }}

{% if var('table_partition_flag') %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['parentAsin', 'childAsin'],
    unique_key = ['date', 'parentAsin', 'childAsin'])}}
{% else %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['date', 'parentAsin', 'childAsin'])}}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%salesandtrafficreportbychildasin%' 
{% endset %}
 
{% set results = run_query(table_name_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}
    {% if var('brand_consolidation_flag') %}
        {% set id =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set id = var('brand_name') %}
    {% endif %}

    SELECT * except(row_num)
        FROM 
        (
    select 
    '{{id}}' as brand,
    CAST(ReportstartDate as timestamp) ReportstartDate,
    CAST(ReportendDate as timestamp) ReportendDate,
    CAST(ReportRequestTime as timestamp) ReportRequestTime,
    sellingPartnerId,
    marketplaceName,
    marketplaceId,
    a.date,
    coalesce(parentAsin,'') as parentAsin,
    coalesce(childAsin,'') as childAsin,
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
        case when c.from_currency_code is null then a.orderedProductSales_currencyCode else c.from_currency_code end as exchange_currency_code , 
    {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        cast(null as string) as exchange_currency_code , 
    {% endif %}
    a._daton_user_id,
    a._daton_batch_runtime,
    a._daton_batch_id,
    {% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
        DATETIME_ADD(cast(a.date as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
    {% else %}
        CAST(a.date as timestamp) as _edm_eff_strt_ts,
    {% endif %}
    null as _edm_eff_end_ts,
    unix_micros(current_timestamp()) as _edm_runtime,
    DENSE_RANK() OVER (PARTITION BY '{{id}}', a.date, parentAsin, childASIN order by a._daton_batch_runtime desc) as row_num
    from {{i}} a 
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and a.orderedProductSales_currencyCode = c.to_currency_code   
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a._daton_batch_runtime  >= {{max_loaded}}
                {% endif %}

            ) where row_num = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

