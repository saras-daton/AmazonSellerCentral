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
    {{set_table_name('%flatfileallordersreportbylastupdate')}}    
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

        SELECT * {{exclude()}} (rank1,rank2)
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY purchase_date, amazon_order_id, asin, sku order by {{daton_batch_runtime()}} desc, quantity desc) _seq_id
            From (
                select *, ROW_NUMBER() OVER (PARTITION BY purchase_date, amazon_order_id, asin, sku order by last_updated_date desc, {{daton_batch_runtime()}} desc) rank2
                From (
                    select
                    '{{brand}}' as brand,
                    '{{store}}' as store,
                    CAST(ReportstartDate as timestamp) ReportstartDate,
                    CAST(ReportendDate as timestamp) ReportendDate,
                    CAST(ReportRequestTime as timestamp) ReportRequestTime,
                    sellingPartnerId
                    marketplaceName,
                    marketplaceId,
                    coalesce(amazon_order_id,'') as amazon_order_id,
                    merchant_order_id,
                    {% if var('timezone_conversion_flag') %}
                        cast(DATETIME_ADD(purchase_date, INTERVAL {{hr}} HOUR ) as DATE) purchase_date,
                        DATETIME_ADD(cast(last_updated_date as timestamp), INTERVAL {{hr}} HOUR ) last_updated_date,
                    {% else %}
                        cast(purchase_date as DATE) purchase_date,
                        CAST(last_updated_date as timestamp) last_updated_date,
                    {% endif %}
                    order_status, 
                    fulfillment_channel, 
                    sales_channel,
                    order_channel,
                    url,
                    ship_service_level,
                    product_name, 
                    coalesce(sku,'') as sku, 
                    coalesce(asin,'') as asin,
                    item_status, 
                    quantity, 
                    currency,
                    item_price, 
                    item_tax, 
                    shipping_price, 
                    shipping_tax, 
                    gift_wrap_price,
                    gift_wrap_tax, 
                    item_promotion_discount, 
                    ship_promotion_discount,
                    address_type,
                    ship_city, 
                    ship_state, 
                    ship_postal_code, 
                    ship_country,  
                    promotion_ids,
                    item_extensions_data,
                    is_business_order,
                    purchase_order_number,
                    price_designation,
                    buyer_company_name,
                    customized_url,
                    customized_page, 
                    is_replacement_order,
                    original_order_id,
                    licensee_name,
                    license_number,
                    license_state,
                    license_expiration_date,
                    {% if var('currency_conversion_flag') %}
                        case when c.value is null then 1 else c.value end as exchange_currency_rate,
                        case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
                    {% else %}
                        cast(1 as decimal) as exchange_currency_rate,
                        cast(null as string) as exchange_currency_code, 
                    {% endif %} 
	   	            a.{{daton_user_id()}},
       	            a.{{daton_batch_runtime()}},
                    a.{{daton_batch_id()}},
                    current_timestamp() as last_updated,
                    null as run_id,
                    ROW_NUMBER() OVER (PARTITION BY last_updated_date, purchase_date, amazon_order_id, asin, sku order by a.{{daton_batch_runtime()}} desc) as rank1
                    from {{i}}  a  
                    {% if var('currency_conversion_flag') %}
                        left join {{ref('ExchangeRates')}} c on date(a.purchase_date) = c.date and a.currency = c.to_currency_code                      
                    {% endif %}
                    {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                    {% endif %}    

                    ) 
                where rank1 = 1 
                ) where rank2 = 1
            )
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
