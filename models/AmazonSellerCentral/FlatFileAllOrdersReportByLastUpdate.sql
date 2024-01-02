{% if var('FlatFileAllOrdersReportByLastUpdate') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("FlatFileAllOrdersReportByLastUpdate_tbl_ptrn","FlatFileAllOrdersReportByLastUpdate_tbl_exclude_ptrn") %}

{% for i in result %}

    select *, row_number() over (partition by purchase_date, amazon_order_id, asin, sku order by _daton_batch_runtime desc, quantity desc) as _seq_id
    from (
        select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,           {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
            {{ timezone_conversion("ReportendDate") }} as ReportendDate,
            {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
            sellingPartnerId
            marketplaceName,
            marketplaceId,
            amazon_order_id,
            merchant_order_id,
            {{ timezone_conversion("purchase_date") }} as purchase_date,
            {{ timezone_conversion("last_updated_date") }} as last_updated_date,
            order_status, 
            fulfillment_channel, 
            sales_channel,
            order_channel,
            url,
            ship_service_level,
            product_name, 
            sku, 
            asin,
            number_of_items,
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
            {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
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
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FlatFileAllOrdersReportByLastUpdate_lookback') }},0) from {{ this }})
            {% endif %}  

            qualify row_number() over (partition by last_updated_date, purchase_date, amazon_order_id, asin, sku order by _daton_batch_runtime desc) = 1 
        )
        qualify row_number() over (partition by purchase_date, amazon_order_id, asin, sku order by last_updated_date desc, _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}