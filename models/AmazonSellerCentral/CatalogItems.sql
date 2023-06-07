{% if var('CatalogItems') %}
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

        SELECT *  {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            CAST(RequeststartDate as timestamp) RequeststartDate,
            CAST(RequestendDate as timestamp) RequestendDate,
            coalesce(ReferenceASIN,'') as ReferenceASIN,
            sellingPartnerId,
            marketplaceName,
            a.marketplaceId,
            asin,
            attributes,
            dimensions,
            identifiers,
            images,
            productTypes,
            relationships,
            {% if target.type=='snowflake' %} 
            salesRanks.VALUE:marketplaceId as salesRanks_marketplaceId,
            classificationRanks.VALUE:classificationId as classificationRanks_classificationId,
            classificationRanks.VALUE:title as classificationRanks_title,
            classificationRanks.VALUE:link as classificationRanks_link,
            classificationRanks.VALUE:rank as classificationRanks_rank,
            displayGroupRanks.VALUE:websiteDisplayGroup as displayGroupRanks_websiteDisplayGroup,
            displayGroupRanks.VALUE:title as displayGroupRanks_title,
            displayGroupRanks.VALUE:link as displayGroupRanks_link,
            displayGroupRanks.VALUE:rank as displayGroupRanks_rank,
            summaries.VALUE:marketplaceId as summaries_marketplaceId,
            summaries.VALUE:brand as brandName,
            summaries.VALUE:browseClassification,
            summaries.VALUE:color as colorName,
            summaries.VALUE:itemClassification,
            summaries.VALUE:itemName as itemName,
            summaries.VALUE:manufacturer as manufacturer,
            coalesce(summaries.VALUE:modelNumber,'') as modelNumber,
            summaries.VALUE:packageQuantity,
            summaries.VALUE:partNumber,
            summaries.VALUE:size as sizeName,
            summaries.VALUE:style as styleName,
            summaries.VALUE:websiteDisplayGroup as summaries_websiteDisplayGroup,
            summaries.VALUE:websiteDisplayGroupName as summaries_websiteDisplayGroupName,
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
            coalesce(summaries.modelNumber,'') as modelNumber,
            summaries.packageQuantity,
            summaries.partNumber,
            summaries.size as sizeName,
            summaries.style as styleName,
            summaries.websiteDisplayGroup as summaries_websiteDisplayGroup,
            summaries.websiteDisplayGroupName as summaries_websiteDisplayGroupName,
            {% endif %}
            vendorDetails,
	        {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            {% if target.type=='snowflake' %} 
            ROW_NUMBER() OVER (PARTITION BY summaries.VALUE:brand,ReferenceASIN,summaries.VALUE:modelNumber,summaries.VALUE:marketplaceId order by {{daton_batch_runtime()}} desc, {{daton_batch_id()}} desc) row_num
            {% else %}
            ROW_NUMBER() OVER (PARTITION BY summaries.brand,ReferenceASIN,summaries.modelNumber,summaries.marketplaceId order by {{daton_batch_runtime()}} desc, {{daton_batch_id()}} desc) row_num
            {% endif %}
    	    from {{i}} a
                {{unnesting("summaries")}}
                {{unnesting("salesRanks")}}
                {{multi_unnesting("salesRanks","classificationRanks")}}
                {{multi_unnesting("salesRanks","displayGroupRanks")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}   
             )
          where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}