-- depends_on: {{ref('ExchangeRates')}}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
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



    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
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

       select * {{exclude()}} (rank,row_num) from (
        Select *, ROW_NUMBER() OVER(PARTITION BY Return_request_date, Order_ID, ASIN order by {{daton_batch_runtime()}} desc, Amazon_RMA_ID desc) as rank 
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            CAST(ReportstartDate as timestamp) ReportstartDate,
            CAST(ReportendDate as timestamp) ReportendDate,
            CAST(ReportRequestTime as timestamp) ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            coalesce(Order_ID,'') as Order_ID,
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
            coalesce(ASIN,'') as ASIN,
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
                cast(null as string) as exchange_currency_code, 
            {% endif %}
	        a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(Order_date as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id,
            {% else %}
                cast(Order_date as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id,
            {% endif %}
            Dense_Rank() OVER (PARTITION BY Return_request_date, Order_ID, ASIN order by a.{{daton_batch_runtime()}} desc) row_num
    	    from {{i}}  a 
                {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.ReportRequestTime) = c.date and a.Currency_code = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}   
            ) where row_num = 1
        )  where rank=1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}