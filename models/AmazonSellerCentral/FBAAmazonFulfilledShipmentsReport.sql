{% if var('FBAAmazonFulfilledShipmentsReport') %}
-- depends_on: {{ref('ExchangeRates')}}

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
    {{set_table_name('%fulfilledshipments%')}}    
    {% endset %} 


    {% set results = run_query(table_name_query) %}
    {% if execute %}
        {# Return the first column #}
        {% set results_list = results.columns[0].values() %}
        {% set tables_lowercase_list = results.columns[1].values() %}
    {% else %}
        {% set results_list = [] %}
        {% set tables_lowercase_list = [] %}
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}


       SELECT *, ROW_NUMBER() OVER (PARTITION BY purchase_date, sku, amazon_order_id order by _daton_batch_runtime, quantity_shipped) as _seq_id
        From (
            SELECT * {{exclude()}} (rank)
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
                coalesce(amazon_order_id,'') as amazon_order_id,
                merchant_order_id,
                shipment_id,
                shipment_item_id,
                amazon_order_item_id,
                merchant_order_item_id,
                CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(purchase_date as timestamp)") }} as {{ dbt.type_timestamp() }}) as purchase_date,
                CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(payments_date as timestamp)") }} as {{ dbt.type_timestamp() }}) as payments_date,
                CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(shipment_date as timestamp)") }} as {{ dbt.type_timestamp() }}) as shipment_date,
                CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(reporting_date as timestamp)") }} as {{ dbt.type_timestamp() }}) as reporting_date,
                buyer_email,
                buyer_name,
                buyer_phone_number,
                coalesce(sku,'') as sku,
                product_name,
                quantity_shipped,
                currency,
                item_price,
                item_tax,
                shipping_price,
                shipping_tax,
                gift_wrap_price,
                gift_wrap_tax,
                ship_service_level,
                recipient_name,
                ship_address_1,
                ship_address_2,
                ship_address_3,
                ship_city,
                ship_state,
                ship_postal_code,
                ship_country,
                ship_phone_number,
                bill_address_1,
                bill_address_2,
                bill_address_3,
                bill_city,
                bill_state,
                bill_postal_code,
                bill_country,
                item_promotion_discount,
                ship_promotion_discount,
                carrier,
                tracking_number,
                estimated_arrival_date,
                fulfillment_center_id,
                fulfillment_channel,
                sales_channel,
	            a.{{daton_user_id()}} as _daton_user_id,
                a.{{daton_batch_runtime()}} as _daton_batch_runtime,
                a.{{daton_batch_id()}} as _daton_batch_id,
                current_timestamp() as _last_updated,
                '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
                ROW_NUMBER() OVER (PARTITION BY purchase_date, sku, amazon_order_id order by a.{{daton_batch_runtime()}} desc) rank
                from {{i}} a
                    {% if var('currency_conversion_flag') %}
                         left join {{ref('ExchangeRates')}} c on date(a.purchase_date) = c.date and a.currency = c.to_currency_code
                    {% endif %}
                    {% if is_incremental() %}
                    {# /* -- this filter will only be applied on an incremental run */ #}
                    WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                    and sales_channel!='Non-Amazon'
                    {% else %}
                    where sales_channel!='Non-Amazon'
                    {% endif %}     
                )
            where rank =1
        )
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
{% endif %}

