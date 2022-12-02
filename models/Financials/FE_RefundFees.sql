-- depends_on: {{ref('ExchangeRates')}}

--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('FE_RefundFees', True)) }}

{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'posteddate', 'data_type': 'date' },
    cluster_by = ['marketplacename', 'amazonorderid'], 
    unique_key = ['posteddate', 'marketplacename', 'amazonorderid', 'FeeType', 'TransactionType', 'AmountType', '_seq_id'])}}

{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['posteddate', 'marketplacename', 'amazonorderid', 'FeeType', 'TransactionType', 'AmountType', '_seq_id'])}}

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

with unnested_refundeventlist as (
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%listfinancialevents%' 
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

    SELECT * FROM (
    select 
    '{{id}}' as Brand,
    {% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
        cast(DATETIME_ADD(cast(RefundEventlist.posteddate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) posteddate,
    {% else %}
        date(RefundEventlist.posteddate) as posteddate,
    {% endif %}
    RefundEventlist.amazonorderid as amazonorderid,
    RefundEventlist.marketplacename as marketplacename,
    RefundEventlist.ShipmentItemAdjustmentList,
    _daton_user_id,
    _daton_batch_runtime,
    _daton_batch_id ,
    FROM  {{i}} cross join unnest(RefundEventlist) RefundEventlist
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

ShipmentItemAdjustmentList as (
        select 
        Brand,
        posteddate, 
        amazonorderid,
        marketplacename,
        ShipmentItemAdjustmentList.sellerSKU as sellerSKU,
        ShipmentItemAdjustmentList.quantityshipped as quantityshipped,
        ShipmentItemAdjustmentList.ItemFeeAdjustmentList,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from unnested_refundeventlist
        cross join unnest(ShipmentItemAdjustmentList) ShipmentItemAdjustmentList
        
),

ItemFeeAdjustmentList as (
        select 
        Brand,
        posteddate, 
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        ItemFeeAdjustmentList.FeeType,
        ItemFeeAdjustmentList.FeeAmount,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from ShipmentItemAdjustmentList
        cross join unnest(ItemFeeAdjustmentList) ItemFeeAdjustmentList
),

FeeAmount as (
        select 
        Brand,
        posteddate,
        'Fees' as AmountType,
        'Refund' as TransactionType,
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        FeeType,
        FeeAmount.CurrencyCode as CurrencyCode,
        FeeAmount.CurrencyAmount as CurrencyAmount,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then FeeAmount.CurrencyCode else c.from_currency_code end as exchange_currency_code , 
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code , 
        {% endif %}
        ItemFeeAdjustmentList._daton_user_id,
        ItemFeeAdjustmentList._daton_batch_runtime,
        ItemFeeAdjustmentList._daton_batch_id,
        {% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
           DATETIME_ADD(cast(posteddate as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(posteddate as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        from ItemFeeAdjustmentList
        cross join unnest(FeeAmount) FeeAmount
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(posteddate) = c.date and FeeAmount.CurrencyCode = c.to_currency_code
        {% endif %}
)

select *, ROW_NUMBER() OVER (PARTITION BY posteddate, marketplacename, amazonorderid order by _daton_batch_runtime, FeeType, TransactionType, AmountType, quantityshipped) _seq_id 
from (
    select * except(rank) from (
        select *,
        DENSE_RANK() OVER (PARTITION BY posteddate, marketplacename, amazonorderid, FeeType, TransactionType, AmountType order by _daton_batch_runtime desc) rank
        from FeeAmount
        ) where rank=1
)
