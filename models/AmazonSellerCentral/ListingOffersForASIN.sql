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
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="Summary.value:OffersAvailableTime") }} as {{ dbt.type_timestamp() }}) as Summary_OffersAvailableTime,
            {% else %}
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(Summary.OffersAvailableTime as timestamp)") }} as {{ dbt.type_timestamp() }}) as Summary_OffersAvailableTime,
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
                where {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            qualify row_number() over (partition by ASIN, itemCondition order by {{daton_batch_runtime()}} desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}