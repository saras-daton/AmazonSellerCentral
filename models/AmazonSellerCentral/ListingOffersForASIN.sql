{% if var('ListingOffersForASIN') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('ListingOffersForASIN_tbl_ptrn'),
exclude=var('ListingOffersForASIN_tbl_exclude_ptrn'),
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
        {{ timezone_conversion("RequestStartDate") }} as RequestStartDate,
        {{ timezone_conversion("RequestEndDate") }} as RequestEndDate,
        ReferenceSKU,
        coalesce(ASIN,'N/A') as ASIN,
        coalesce(SKU,'N/A') as SKU,
        itemCondition,
        customerType,
        status,
        marketplaceId,
        sellingPartnerId,
        {{extract_nested_value("Summary","TotalOfferCount","integer")}} as Summary_TotalOfferCount,
        {{extract_nested_value("BuyBoxPrices","condition","string")}} as BuyBoxPrices_condition,
        {{extract_nested_value("BuyBoxPrices","offerType","string")}} as BuyBoxPrices_offerType,
        {{extract_nested_value("BuyBoxPrices","quantityTier","integer")}} as BuyBoxPrices_quantityTier,
        {{extract_nested_value("BuyBoxPrices","quantityDiscountType","string")}} as BuyBoxPrices_quantityDiscountType,
        {{extract_nested_value("LandedPrice","CurrencyCode","string")}} as LandedPrice_CurrencyCode,
        {{extract_nested_value("LandedPrice","Amount","numeric")}} as LandedPrice_Amount,
        {{extract_nested_value("ListingPrice","CurrencyCode","string")}} as ListingPrice_CurrencyCode,
        {{extract_nested_value("ListingPrice","Amount","numeric")}} as ListingPrice_Amount,
        {{extract_nested_value("BuyBoxPrices","sellerId","string")}} as BuyBoxPrices_sellerId,
        {% if target.type=='snowflake' %} 
            {{ timezone_conversion("Summary.value:OffersAvailableTime") }} as Summary_OffersAvailableTime,
        {% else %}
            {{ timezone_conversion("Summary.OffersAvailableTime") }} as Summary_OffersAvailableTime,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} 
            {{unnesting("Summary")}}
            {{multi_unnesting("Summary","BuyBoxPrices")}}
            {{multi_unnesting("BuyBoxPrices","LandedPrice")}}            
            {{multi_unnesting("BuyBoxPrices","ListingPrice")}}            
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('ListingOffersForASIN_lookback') }},0) from {{ this }})
            {% endif %} 
        qualify row_number() over (partition by ASIN, itemCondition order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}