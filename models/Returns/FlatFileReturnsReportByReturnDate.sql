-- depends_on: {{ref('ExchangeRates')}}

--To disable the model, set the model name variable as False within your dbt_project.yml file.
{{ config(enabled=var('FlatFileReturnsReportByReturnDate', True)) }}

{% if var('table_partition_flag') %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by = { 'field': 'Return_request_date', 'data_type': 'date' },
    cluster_by = ['ASIN','Merchant_SKU', 'Order_ID'],
    unique_key = ['Return_request_date', 'Order_ID', 'ASIN'])}}
{% else %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['Return_request_date', 'Order_ID', 'ASIN'])}}
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

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%flatfilereturnsreportbyreturndate' 
{% endset %}  


{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('brand_consolidation_flag') %}
        {% set id =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set id = var('brand_name') %}
    {% endif %}

    {% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}

    select * except(rank, row_num) from (
    Select *, Dense_Rank() OVER(PARTITION BY Return_request_date, Order_ID, ASIN order by _daton_batch_runtime desc, Amazon_RMA_ID desc) as rank 
    From (
        select '{{id}}' as brand,
        CAST(ReportstartDate as timestamp) ReportstartDate,
        CAST(ReportendDate as timestamp) ReportendDate,
        CAST(ReportRequestTime as timestamp) ReportRequestTime,
        sellingPartnerId,
        marketplaceName,
        marketplaceId,
        Order_ID,
        Order_date,
        Return_request_date,
		Return_request_status,
        Amazon_RMA_ID,
        Merchant_RMA_ID,
        Seller_RMA_ID,
        Label_type,
        Label_cost,
        Currency_code,
        Return_carrier,
        Tracking_ID
        Label_to_be_paid_by,
        A_to_z_claim,
        Is_prime,
        ASIN,
        Merchant_SKU,
        Item_Name,
        Return_quantity,
        Return_Reason,
        In_policy,
        Return_type,
        Resolution,
        Invoice_number,
        Return_delivery_date,
        Order_Amount,
        Order_quantity,
        SafeT_action_reason,
        SafeT_claim_id,
        SafeT_claim_state,
        SafeT_claim_creation_time,
        SafeT_claim_reimbursement_amount,
        Refunded_Amount,
        Category,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as conversion_rate,
            case when c.from_currency_code is null then a.Currency_code else c.from_currency_code end as conversion_currency, 
        {% else %}
            cast(1 as decimal) as conversion_rate,
            cast(null as string) as conversion_currency, 
        {% endif %}
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,		
		{% if var('timezone_conversion_flag')['amazon_sellerpartner'] %}
            DATETIME_ADD(TIMESTAMP_MILLIS(cast(a._daton_batch_runtime as int)), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
            TIMESTAMP_MILLIS(cast(a._daton_batch_runtime as int)) _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        Dense_Rank() OVER (PARTITION BY Return_request_date, Order_ID, ASIN order by a._daton_batch_runtime desc) row_num
	    from {{i}} a 
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.ReportRequestTime) = c.date and a.Currency_code = c.to_currency_code
            {% endif %}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a._daton_batch_runtime  >= {{max_loaded}}
            {% endif %}    
        ) where row_num = 1
    )  where rank=1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
