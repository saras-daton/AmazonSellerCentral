{% if var('CatalogItems') %}
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
    {{set_table_name('%catalogitems')}}    
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
            coalesce(ReferenceASIN,'N/A') as ReferenceASIN,
            sellingPartnerId,
            marketplaceName,
            a.marketplaceId,
            asin,
            {% if target.type=='snowflake' %} 
            salesRanks.value:marketplaceId as salesRanks_marketplaceId,
            classificationRanks.value:classificationId as classificationRanks_classificationId,
            classificationRanks.value:title as classificationRanks_title,
            classificationRanks.value:link as classificationRanks_link,
            classificationRanks.value:rank as classificationRanks_rank,
            displayGroupRanks.value:websiteDisplayGroup as displayGroupRanks_websiteDisplayGroup,
            displayGroupRanks.value:title as displayGroupRanks_title,
            displayGroupRanks.value:link as displayGroupRanks_link,
            displayGroupRanks.value:rank as displayGroupRanks_rank,
            summaries.value:marketplaceId as summaries_marketplaceId,
            summaries.value:brand as brandName,
            summaries.value:browseClassification,
            summaries.value:color as colorName,
            summaries.value:itemClassification,
            summaries.value:itemName as itemName,
            summaries.value:manufacturer as manufacturer,
            coalesce(summaries.value:modelNumber,'N/A') as modelNumber,
            summaries.value:packageQuantity,
            summaries.value:partNumber,
            summaries.value:size as sizeName,
            summaries.value:style as styleName,
            summaries.value:websiteDisplayGroup as summaries_websiteDisplayGroup,
            summaries.value:websiteDisplayGroupName as summaries_websiteDisplayGroupName,
            {% else %}
            salesRanks.marketplaceId as salesRanks_marketplaceId,
            classificationRanks.classificationId as classificationRanks_classificationId,
            classificationRanks.title as classificationRanks_title,
            classificationRanks.link as classificationRanks_link,
            classificationRanks.rank as classificationRanks_rank,
            displayGroupRanks.websiteDisplayGroup as displayGroupRanks_websiteDisplayGroup,
            displayGroupRanks.title as displayGroupRanks_title,
            displayGroupRanks.link as displayGroupRanks_link,
            displayGroupRanks.rank as displayGroupRanks_rank,
            summaries.marketplaceId as summaries_marketplaceId,
            summaries.brand as brandName,
            summaries.browseClassification,
            summaries.color	as colorName,
            summaries.itemClassification,
            summaries.itemName as itemName,
            summaries.manufacturer as manufacturer,
            coalesce(summaries.modelNumber,'N/A') as modelNumber,
            summaries.packageQuantity,
            summaries.partNumber,
            summaries.size as sizeName,
            summaries.style as styleName,
            summaries.websiteDisplayGroup as summaries_websiteDisplayGroup,
            summaries.websiteDisplayGroupName as summaries_websiteDisplayGroupName,
            {% endif %}
	        {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    	    from {{i}} a
                {{unnesting("summaries")}}
                {{unnesting("salesRanks")}}
                {{multi_unnesting("salesRanks","classificationRanks")}}
                {{multi_unnesting("salesRanks","displayGroupRanks")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}   
            {% if target.type=='snowflake' %} 
            qualify row_number() over (partition by summaries.value:brand,ReferenceASIN,summaries.value:modelNumber,summaries.value:marketplaceId order by {{daton_batch_runtime()}} desc, {{daton_batch_id()}} desc) = 1
            {% else %}
            qualify row_number() over (partition by summaries.brand,ReferenceASIN,summaries.modelNumber,summaries.marketplaceId order by {{daton_batch_runtime()}} desc, {{daton_batch_id()}} desc) = 1
            {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}