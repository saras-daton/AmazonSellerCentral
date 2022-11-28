-- depends_on: {{ref('ExchangeRates')}}

--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('ListFinancialEvents_OrderTaxes', True)) }}

{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'posteddate', 'data_type': 'date' },
    cluster_by = ['marketplacename', 'amazonorderid'], 
    unique_key = ['posteddate', 'marketplacename', 'amazonorderid', 'ChargeType', 'TransactionType', 'AmountType', '_seq_id'])}}
{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['posteddate', 'marketplacename', 'amazonorderid', 'ChargeType', 'TransactionType', 'AmountType', '_seq_id'])}}
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

with unnested_shipmenteventlist as (
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
        cast(DATETIME_ADD(cast(ShipmentEventlist.posteddate as timestamp), INTERVAL {{hr}} HOUR ) as DATE) posteddate,
    {% else %}
        date(ShipmentEventlist.posteddate) as posteddate,
    {% endif %}
    ShipmentEventlist.amazonorderid as amazonorderid,
    ShipmentEventlist.marketplacename as marketplacename,
    ShipmentEventlist.ShipmentItemList,
    _daton_user_id,
    _daton_batch_runtime,
    _daton_batch_id, 
    FROM  {{i}} cross join unnest(ShipmentEventlist) ShipmentEventlist
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %}
        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

ShipmentItemList as (
        select 
        Brand, 
        posteddate,
        amazonorderid,
        marketplacename,
        ShipmentItemList.sellerSKU as sellerSKU,
        ShipmentItemList.quantityshipped as quantityshipped,
        ShipmentItemList.ItemTaxWithHeldList,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from unnested_shipmenteventlist
        cross join unnest(ShipmentItemList) ShipmentItemList        
),

ItemTaxWithHeldList as (
        select 
        Brand,
        posteddate, 
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        ItemTaxWithHeldList.TaxesWithheld,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from ShipmentItemList
        cross join unnest(ItemTaxWithHeldList) ItemTaxWithHeldList
),

TaxesWithheld as (
        select 
        Brand, 
        posteddate,
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        TaxesWithheld.ChargeType,
        TaxesWithheld.ChargeAmount,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from ItemTaxWithHeldList
        cross join unnest(TaxesWithheld) TaxesWithheld
),

ChargeAmount as (
        select 
        Brand,
        posteddate,
        'Taxes' as AmountType,
        'Order' as TransactionType,
        amazonorderid,
        marketplacename,
        sellerSKU,
        quantityshipped,
        ChargeType,
        ChargeAmount.CurrencyCode as CurrencyCode,
        ChargeAmount.CurrencyAmount as CurrencyAmount,
        {% if var('currency_conversion_flag') %}
            c.value as conversion_rate,
            c.from_currency_code as conversion_currency, 
        {% else %}
            cast(1 as decimal) as conversion_rate,
            cast(null as string) as conversion_currency, 
        {% endif %}
        TaxesWithheld._daton_user_id,
        TaxesWithheld._daton_batch_runtime,
        TaxesWithheld._daton_batch_id,
        {% if var('timezone_conversion_flag') %}
           DATETIME_ADD(cast(posteddate as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(posteddate as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        from TaxesWithheld
        cross join unnest(ChargeAmount) ChargeAmount
        {% if var('currency_conversion_flag') %}
            left join {{ var('stg_projectid') }}.{{ var('stg_dataset_common') }}.ExchangeRates c on date(posteddate) = c.date and ChargeAmount.CurrencyCode = c.to_currency_code
        {% endif %}
)

select *, ROW_NUMBER() OVER (PARTITION BY posteddate, marketplacename, amazonorderid order by _daton_batch_runtime, ChargeType, TransactionType, AmountType, quantityshipped) _seq_id
from (
    select * except(rank) from (
        select *,
        DENSE_RANK() OVER (PARTITION BY posteddate, marketplacename, amazonorderid, ChargeType, TransactionType, AmountType order by _daton_batch_runtime desc) rank
        from ChargeAmount
    ) where rank=1
)
