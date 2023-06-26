{% if var('ListingOffersForASIN') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

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

        SELECT *  {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            CAST(RequeststartDate as timestamp) RequeststartDate,
            CAST(RequestendDate as timestamp) RequestendDate,
            ReferenceSKU,
            coalesce(ASIN,'') as ASIN,
            coalesce(SKU,'') as SKU,
            itemCondition,
            customerType,
            status,
            marketplaceId,
            sellingPartnerId,
            Identifier,
            {% if target.type=='snowflake' %} 
            Summary.VALUE:TotalOfferCount as Summary_TotalOfferCount,
            Summary.VALUE:NumberOfOffers as Summary_NumberOfOffers,
            Summary.VALUE:LowestPrices as Summary_LowestPrices,
            BuyBoxPrices.VALUE:condition as BuyBoxPrices_condition,
            BuyBoxPrices.VALUE:offerType as BuyBoxPrices_offerType,
            BuyBoxPrices.VALUE:quantityTier as BuyBoxPrices_quantityTier,
            BuyBoxPrices.VALUE:quantityDiscountType as BuyBoxPrices_quantityDiscountType,
            LandedPrice.VALUE:CurrencyCode as LandedPrice_CurrencyCode,
            LandedPrice.VALUE:Amount as LandedPrice_Amount,
            ListingPrice.VALUE:CurrencyCode as ListingPrice_CurrencyCode,
            ListingPrice.VALUE:Amount as ListingPrice_Amount,
            BuyBoxPrices.VALUE:Shipping as BuyBoxPrices_Shipping,
            BuyBoxPrices.VALUE:Points as BuyBoxPrices_Points,
            BuyBoxPrices.VALUE:sellerId as BuyBoxPrices_sellerId,
            Summary.VALUE:ListPrice as Summary_ListPrice,
            Summary.VALUE:CompetitivePriceThreshold as Summary_CompetitivePriceThreshold,
            Summary.VALUE:SuggestedLowerPricePlusShipping as Summary_SuggestedLowerPricePlusShipping,
            Summary.VALUE:SalesRankings as Summary_SalesRankings,
            Summary.VALUE:BuyBoxEligibleOffers as Summary_BuyBoxEligibleOffers,
            Summary.VALUE:OffersAvailableTime as Summary_OffersAvailableTime,
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
            Offers,
	        {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            ROW_NUMBER() OVER (PARTITION BY ASIN,itemCondition order by {{daton_batch_runtime()}} desc) row_num
    	    from {{i}} 
                {{unnesting("Summary")}}
                {{multi_unnesting("Summary","BuyBoxPrices")}}
                {{multi_unnesting("BuyBoxPrices","LandedPrice")}}            
                {{multi_unnesting("BuyBoxPrices","ListingPrice")}}            
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}   
             )
          where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}