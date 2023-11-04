{% if var('FlatFileReturnsReportByReturnDate') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('FlatFileReturnsReportByReturnDate_tbl_ptrn'),
exclude=var('FlatFileReturnsReportByReturnDate_tbl_exclude_ptrn'),
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

    select *
    from (
        select *
        from (
            select 
            '{{brand|replace("`","")}}' as brand,
            '{{store|replace("`","")}}' as store,
            {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
            {{ timezone_conversion("ReportendDate") }} as ReportendDate,
            {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            coalesce(Order_ID,'N/A') as Order_ID,
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
            coalesce(ASIN,'N/A') as ASIN,
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
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.Currency_code else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                a.Currency_code as exchange_currency_code, 
            {% endif %}
            a.{{daton_user_id()}} as _daton_user_id,
            a.{{daton_batch_runtime()}} as _daton_batch_runtime,
            a.{{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
            from {{i}}  a 
                {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.ReportRequestTime) = c.date and a.Currency_code = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                    {# /* -- this filter will only be applied on an incremental run */ #}
                    where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FlatFileReturnsReportByReturnDate_lookback') }},0) from {{ this }})
                {% endif %}  
        ) 
        qualify dense_rank() over (partition by Return_request_date, Order_ID, ASIN, marketplaceId order by _daton_batch_runtime desc) = 1
    )
    qualify row_number() over(partition by Return_request_date, Order_ID, ASIN order by _daton_batch_runtime desc, Amazon_RMA_ID desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}