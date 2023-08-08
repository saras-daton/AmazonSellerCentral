{% if var('FlatFileReturnsReportByReturnDate') %}
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

    {% set table_name_query %}
    {{set_table_name('%flatfilereturnsreportbyreturndate')}}    
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
            select * from (
                select 
                '{{brand}}' as brand,
                '{{store}}' as store,
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportstartDate") }} as {{ dbt.type_timestamp() }}) as ReportstartDate,
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportendDate") }} as {{ dbt.type_timestamp() }}) as ReportendDate,
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportRequestTime") }} as {{ dbt.type_timestamp() }}) as ReportRequestTime,
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
                    where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                    {% endif %}   
            ) 
            qualify dense_rank() over (partition by Return_request_date, Order_ID, ASIN, marketplaceId order by _daton_batch_runtime desc) = 1
        )
        qualify row_number() over(partition by Return_request_date, Order_ID, ASIN order by _daton_batch_runtime desc, Amazon_RMA_ID desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}