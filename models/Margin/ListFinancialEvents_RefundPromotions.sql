-- depends_on: {{ref('ExchangeRates')}}

{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'posteddate', 'data_type': 'date' },
    cluster_by = ['marketplacename', 'amazonorderid'], 
    unique_key = ['posteddate', 'marketplacename', 'amazonorderid', 'PromotionType', 'TransactionType', 'AmountType', '_seq_id'])}}

{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['posteddate', 'marketplacename', 'amazonorderid', 'PromotionType', 'TransactionType', 'AmountType', '_seq_id'])}}

{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT MAX(_daton_batch_runtime) - 2592000000 FROM {{ this }}
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


{% if var('timezone_conversion_flag') %}
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
    {% if var('timezone_conversion_flag') %}
    cast(DATETIME_ADD(cast(RefundEventlist.posteddate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) posteddate,
    {% else %}
    date(RefundEventlist.posteddate) as posteddate,
    {% endif %}
    RefundEventlist.amazonorderid as amazonorderid,
    RefundEventlist.marketplacename as marketplacename,
    RefundEventlist.ShipmentItemAdjustmentList,
    _daton_user_id,
    _daton_batch_runtime,
    _daton_batch_id,
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
        ShipmentItemAdjustmentList.PromotionList,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id  
        from unnested_refundeventlist
        cross join unnest(ShipmentItemAdjustmentList) ShipmentItemAdjustmentList        
),

PromotionList as (
        select 
        Brand,
        posteddate, 
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        PromotionList.PromotionAmount,
        PromotionList.PromotionType,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id  
        from ShipmentItemAdjustmentList
        cross join unnest(PromotionList) PromotionList
),

PromotionAmount as (
        select 
        Brand,
        posteddate, 
        'Promotion' as AmountType,
        'Refund' as TransactionType,
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        PromotionType,
        PromotionAmount.CurrencyCode as CurrencyCode,
        PromotionAmount.CurrencyAmount as CurrencyAmount,
        {% if var('currency_conversion_flag') %}
            c.value as conversion_rate,
            c.from_currency_code as conversion_currency, 
        {% else %}
            cast(1 as decimal) as conversion_rate,
            cast(null as string) as conversion_currency,
        {% endif %}
        PromotionList._daton_user_id,
        PromotionList._daton_batch_runtime,
        PromotionList._daton_batch_id,
        {% if var('timezone_conversion_flag') %}
           DATETIME_ADD(cast(posteddate as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(posteddate as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        from PromotionList
        cross join unnest(PromotionAmount) PromotionAmount
        {% if var('currency_conversion_flag') %}
            left join {{ var('stg_projectid') }}.{{ var('stg_dataset_common') }}.ExchangeRates c on date(posteddate) = c.date and PromotionAmount.CurrencyCode = c.to_currency_code
        {% endif %}
)

select *, ROW_NUMBER() OVER (PARTITION BY posteddate, marketplacename, amazonorderid order by _daton_batch_runtime, PromotionType, TransactionType, AmountType, quantityshipped) _seq_id
from (
    select * except(rank) from (
        select *,
        DENSE_RANK() OVER (PARTITION BY posteddate, marketplacename, amazonorderid, PromotionType, TransactionType, AmountType order by _daton_batch_runtime desc) rank
        from PromotionAmount
        ) where rank=1
)