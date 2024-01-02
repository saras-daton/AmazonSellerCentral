{% if var('FBAAmazonFulfilledShipmentsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("FBAAmazonFulfilledShipmentsReport_tbl_ptrn","FBAAmazonFulfilledShipmentsReport_tbl_exclude_ptrn") %}

{% for i in result %}

 
    select *, row_number() over (partition by purchase_date, sku, amazon_order_id order by _daton_batch_runtime, quantity_shipped) as _seq_id
    from (
       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,
        {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
        {{ timezone_conversion("ReportendDate") }} as ReportendDate,
        {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
        sellingPartnerId,
        marketplaceName,
        marketplaceId,
        amazon_order_id,
        merchant_order_id,
        shipment_id,
        shipment_item_id,
        amazon_order_item_id,
        merchant_order_item_id,
        {{ timezone_conversion("purchase_date") }} as purchase_date,
        {{ timezone_conversion("payments_date") }} as payments_date,
        {{ timezone_conversion("shipment_date") }} as shipment_date,
        {{ timezone_conversion("reporting_date") }} as reporting_date,
        buyer_email,
        buyer_name,
        buyer_phone_number,
        sku,
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
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.purchase_date) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAAmazonFulfilledShipmentsReport_lookback') }},0) from {{ this }})
                and sales_channel!='Non-Amazon'
            {% else %}
                where sales_channel!='Non-Amazon'
            {% endif %}     
        qualify row_number() over (partition by purchase_date, sku, amazon_order_id,marketplaceName order by a.{{daton_batch_runtime()}} desc) = 1
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}