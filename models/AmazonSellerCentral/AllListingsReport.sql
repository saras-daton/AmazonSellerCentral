{% if var('AllListingsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('AllListingsReport_tbl_ptrn'),
exclude=var('AllListingsReport_tbl_exclude_ptrn'),
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

    select 
    '{{brand|replace("`","")}}' as brand,
    '{{store|replace("`","")}}' as store,
    {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
    {{ timezone_conversion("ReportendDate") }} as ReportendDate,
    {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
    sellingPartnerId,
    marketplaceName,
    marketplaceId,
    item_name,
    item_description,
    listing_id,
    seller_sku,
    price,
    quantity,
    {{ timezone_conversion("open_date") }} as open_date,
    image_url,
    item_is_marketplace,
    product_id_type,
    zshop_shipping_fee,
    item_note,
    item_condition,
    zshop_category1,
    zshop_browse_path,
    zshop_storefront_feature,
    asin1,
    asin2,
    asin3,
    will_ship_internationally,
    expedited_shipping,
    zshop_boldface,
    product_id,
    bid_for_featured_placement,
    add_delete,
    pending_quantity,
    fulfillment_channel,
    optional_payment_type_exclusion,
    merchant_shipping_group,
    status,
    maximum_retail_price,
    scheduled_delivery_sku_set,
    standard_price_point,
    ProductTaxCode,
    minimum_seller_allowed_price,
    maximum_seller_allowed_price,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('AllListingsReport_lookback') }},0) from {{ this }})
        {% endif %}       
    qualify dense_rank() over (partition by seller_sku, listing_id order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
