{% if var('FBAAmazonFulfilledShipmentsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('FBAAmazonFulfilledShipmentsReport_tbl_ptrn'),
exclude=var('FBAAmazonFulfilledShipmentsReport_tbl_exclude_ptrn'),
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

    select *, row_number() over (partition by purchase_date, sku, amazon_order_id order by _daton_batch_runtime, quantity_shipped) as _seq_id
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
        coalesce(amazon_order_id,'N/A') as amazon_order_id,
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
        coalesce(sku,'N/A') as sku,
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
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.purchase_date) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAAmazonFulfilledShipmentsReport_lookback') }},0) from {{ this }})
                and sales_channel!='Non-Amazon'
            {% else %}
                where sales_channel!='Non-Amazon'
            {% endif %}     
        qualify row_number() over (partition by purchase_date, sku, amazon_order_id,marketplaceName order by a.{{daton_batch_runtime()}} desc) = 1
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}