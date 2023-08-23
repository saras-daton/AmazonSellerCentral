{% if var('FlatFileAllOrdersReportByLastUpdate') %}
    {{ config( enabled = True,
    post_hook = "drop table {{this|replace('FlatFileAllOrdersReportByLastUpdate', 'FlatFileAllOrdersReportByLastUpdate_temp')}}"
    ) }}
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
        {{set_table_name('%flatfileallordersreportbylastupdate')}}    
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

        {% if i==results_list[0] %}
            {% set action1 = 'create or replace table' %}
            {% set tbl = this ~ ' as ' %}
        {% else %}
            {% set action1 = 'insert into ' %}
            {% set tbl = this %}
        {% endif %}

        {%- set query -%}
        {{action1}}
        {{tbl|replace('FlatFileAllOrdersReportByLastUpdate', 'FlatFileAllOrdersReportByLastUpdate_temp')}}

        select *, row_number() over (partition by purchase_date, amazon_order_id, asin, sku order by _daton_batch_runtime desc, quantity desc) as _seq_id
            from (
                select *
                from (
                    select
                    '{{brand}}' as brand,
                    '{{store}}' as store,
                    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportstartDate") }} as {{ dbt.type_timestamp() }}) as ReportstartDate,
                    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportendDate") }} as {{ dbt.type_timestamp() }}) as ReportendDate,
                    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportRequestTime") }} as {{ dbt.type_timestamp() }}) as ReportRequestTime,
                    sellingPartnerId
                    marketplaceName,
                    marketplaceId,
                    coalesce(amazon_order_id,'N/A') as amazon_order_id,
                    merchant_order_id,
                    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="purchase_date") }} as {{ dbt.type_timestamp() }}) as purchase_date,
                    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(last_updated_date as timestamp)") }} as {{ dbt.type_timestamp() }}) as last_updated_date,
                    order_status, 
                    fulfillment_channel, 
                    sales_channel,
                    order_channel,
                    url,
                    ship_service_level,
                    product_name, 
                    coalesce(sku,'N/A') as sku, 
                    coalesce(asin,'N/A') as asin,
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
                        a.currency as exchange_currency_code, 
                    {% endif %} 
	   	            a.{{daton_user_id()}} as _daton_user_id,
                    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
                    a.{{daton_batch_id()}} as _daton_batch_id,
                    current_timestamp() as _last_updated,
                    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
                    from {{i}}  a  
                    {% if var('currency_conversion_flag') %}
                        left join {{ref('ExchangeRates')}} c on date(a.purchase_date) = c.date and a.currency = c.to_currency_code                      
                    {% endif %}
                    {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                    {% endif %}    

                    ) 
                    qualify row_number() over (partition by last_updated_date, purchase_date, amazon_order_id, asin, sku order by _daton_batch_runtime desc) = 1 
                )
                qualify row_number() over (partition by purchase_date, amazon_order_id, asin, sku order by last_updated_date desc, _daton_batch_runtime desc) = 1
    {% endset %}

    {% do run_query(query) %}

    {% endfor %}
    select * from {{this|replace('FlatFileAllOrdersReportByLastUpdate', 'FlatFileAllOrdersReportByLastUpdate_temp')}}    
