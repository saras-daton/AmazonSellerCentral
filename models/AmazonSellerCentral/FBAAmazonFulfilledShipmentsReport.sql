-- depends_on: {{ref('ExchangeRates')}}
    {% if var('table_partition_flag') %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        partition_by = { 'field': 'purchase_date', 'data_type': 'date' },
        cluster_by = ['sku','amazon_order_id'],
        unique_key = ['purchase_date', 'sku', 'amazon_order_id', '_seq_id'])}}
    {% else %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['purchase_date', 'sku', 'amazon_order_id', '_seq_id'])}}
    {% endif %}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}} ) - 2592000000,0) FROM {{ this }}
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
    {% else %}
    {% set results_list = [] %}
    {% endif %}


    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}

    {% for i in results_list %}
        {% if var('brand_consolidation_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brand_name_position')] %}
        {% else %}
            {% set brand = var('brand_name') %}
        {% endif %}

        {% if var('store_consolidation_flag') %}
            {% set store =i.split('.')[2].split('_')[var('store_name_position')] %}
        {% else %}
            {% set store = var('store') %}
        {% endif %}

       SELECT *, ROW_NUMBER() OVER (PARTITION BY purchase_date, sku, amazon_order_id order by {{daton_batch_runtime()}}, quantity_shipped) _seq_id
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
                {% if var('timezone_conversion_flag') %}
                    cast(DATETIME_ADD(cast(purchase_date as timestamp), INTERVAL {{hr}} HOUR ) as DATE) purchase_date,
                    cast(DATETIME_ADD(cast(payments_date as timestamp), INTERVAL {{hr}} HOUR ) as DATE) payments_date,
                    cast(DATETIME_ADD(cast(shipment_date as timestamp), INTERVAL {{hr}} HOUR ) as DATE) shipment_date,
                    cast(DATETIME_ADD(cast(reporting_date as timestamp), INTERVAL {{hr}} HOUR ) as DATE) reporting_date,
                {% else %}
                    cast(cast(purchase_date as timestamp) as date) purchase_date,
                    cast(cast(payments_date as timestamp) as date) payments_date,
                    cast(cast(shipment_date as timestamp) as date) shipment_date,
                    cast(cast(reporting_date as timestamp) as date) reporting_date,
                {% endif %}
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
	            a.{{daton_user_id()}},
                a.{{daton_batch_runtime()}},
                a.{{daton_batch_id()}},
	            {% if var('timezone_conversion_flag') %}
                    DATETIME_ADD(cast(a.purchase_date as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                    null as effective_end_date,
                    DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                    null as run_id,
                {% else %}
                    cast(a.purchase_date as timestamp) as effective_start_date,
                    null as effective_end_date,
                    current_timestamp() as last_updated,
                    null as run_id,
                {% endif %}
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


