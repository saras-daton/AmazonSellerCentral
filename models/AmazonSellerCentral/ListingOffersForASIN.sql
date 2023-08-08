{% if var('ListingOffersForASIN') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
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
    {{set_table_name('%listingoffersforasin')}}    
    {% endset %}   


    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
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

        
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(RequeststartDate as timestamp)") }} as {{ dbt.type_timestamp() }}) as RequeststartDate,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(RequestendDate as timestamp)") }} as {{ dbt.type_timestamp() }}) as RequestendDate,
            ReferenceSKU,
            coalesce(ASIN,'N/A') as ASIN,
            coalesce(SKU,'N/A') as SKU,
            itemCondition,
            customerType,
            status,
            marketplaceId,
            sellingPartnerId,
            {% if target.type=='snowflake' %} 
            Summary.value:TotalOfferCount as Summary_TotalOfferCount,
            Summary.value:NumberOfOffers as Summary_NumberOfOffers,
            Summary.value:LowestPrices as Summary_LowestPrices,
            BuyBoxPrices.value:condition as BuyBoxPrices_condition,
            BuyBoxPrices.value:offerType as BuyBoxPrices_offerType,
            BuyBoxPrices.value:quantityTier as BuyBoxPrices_quantityTier,
            BuyBoxPrices.value:quantityDiscountType as BuyBoxPrices_quantityDiscountType,
            LandedPrice.value:CurrencyCode as LandedPrice_CurrencyCode,
            LandedPrice.value:Amount as LandedPrice_Amount,
            ListingPrice.value:CurrencyCode as ListingPrice_CurrencyCode,
            ListingPrice.value:Amount as ListingPrice_Amount,
            BuyBoxPrices.value:Shipping as BuyBoxPrices_Shipping,
            BuyBoxPrices.value:Points as BuyBoxPrices_Points,
            BuyBoxPrices.value:sellerId as BuyBoxPrices_sellerId,
            Summary.value:ListPrice as Summary_ListPrice,
            Summary.value:CompetitivePriceThreshold as Summary_CompetitivePriceThreshold,
            Summary.value:SuggestedLowerPricePlusShipping as Summary_SuggestedLowerPricePlusShipping,
            Summary.value:SalesRankings as Summary_SalesRankings,
            Summary.value:BuyBoxEligibleOffers as Summary_BuyBoxEligibleOffers,
            Summary.value:OffersAvailableTime as Summary_OffersAvailableTime,
            {% else %}
            Summary.TotalOfferCount as Summary_TotalOfferCount,
            Summary.NumberOfOffers as Summary_NumberOfOffers,
            Summary.LowestPrices as Summary_LowestPrices,
            BuyBoxPrices.condition as BuyBoxPrices_condition,
            BuyBoxPrices.offerType as BuyBoxPrices_offerType,
            BuyBoxPrices.quantityTier as BuyBoxPrices_quantityTier,
            BuyBoxPrices.quantityDiscountType as BuyBoxPrices_quantityDiscountType,
            LandedPrice.CurrencyCode as LandedPrice_CurrencyCode,
            LandedPrice.Amount as LandedPrice_Amount,
            ListingPrice.CurrencyCode as ListingPrice_CurrencyCode,
            ListingPrice.Amount as ListingPrice_Amount,
            BuyBoxPrices.Shipping as BuyBoxPrices_Shipping,
            BuyBoxPrices.Points as BuyBoxPrices_Points,
            BuyBoxPrices.sellerId as BuyBoxPrices_sellerId,
            Summary.ListPrice as Summary_ListPrice,
            Summary.CompetitivePriceThreshold as Summary_CompetitivePriceThreshold,
            Summary.SuggestedLowerPricePlusShipping as Summary_SuggestedLowerPricePlusShipping,
            Summary.SalesRankings as Summary_SalesRankings,
            Summary.BuyBoxEligibleOffers as Summary_BuyBoxEligibleOffers,
            Summary.OffersAvailableTime as Summary_OffersAvailableTime,
            {% endif %}
	        {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            from {{i}} 
                {{unnesting("Summary")}}
                {{multi_unnesting("Summary","BuyBoxPrices")}}
                {{multi_unnesting("BuyBoxPrices","LandedPrice")}}            
                {{multi_unnesting("BuyBoxPrices","ListingPrice")}}            
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            qualify row_number() over (partition by ASIN, itemCondition order by {{daton_batch_runtime()}} desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}