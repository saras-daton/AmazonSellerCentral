{% if var('CatalogItems') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% set result =set_table_name("CatalogItems_tbl_ptrn","CatalogItems_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,
    {{ timezone_conversion("RequeststartDate") }} as RequeststartDate,
    {{ timezone_conversion("RequestendDate") }} as RequestendDate,
    ReferenceASIN,
    sellingPartnerId,
    marketplaceName,
    a.marketplaceId,
    asin,
    {{extract_nested_value("salesRanks","marketplaceId","string")}} as salesRanks_marketplaceId,
    {{extract_nested_value("classificationRanks","classificationId","string")}} as classificationRanks_classificationId,
    {{extract_nested_value("classificationRanks","title","string")}} as classificationRanks_title,
    {{extract_nested_value("classificationRanks","link","string")}} as classificationRanks_link,
    {{extract_nested_value("classificationRanks","rank","integer")}} as classificationRanks_rank,
    {{extract_nested_value("displayGroupRanks","websiteDisplayGroup","string")}} as displayGroupRanks_websiteDisplayGroup,
    {{extract_nested_value("displayGroupRanks","title","string")}} as displayGroupRanks_title,
    {{extract_nested_value("displayGroupRanks","link","string")}} as displayGroupRanks_link,
    {{extract_nested_value("displayGroupRanks","rank","integer")}} as displayGroupRanks_rank,
    {{extract_nested_value("summaries","marketplaceId","string")}} as summaries_marketplaceId,
    {{extract_nested_value("summaries","brand","string")}} as summaries_brandName,
    
    {{extract_nested_value("browseClassification","displayName","string")}} as summaries_browseClassification_diplayName,
    {{extract_nested_value("browseClassification","classificationId","string")}} as summaries_browseClassification_classificationId,
    
    {{extract_nested_value("summaries","color","string")}} as summaries_colorName,
    {{extract_nested_value("summaries","itemClassification","string")}} as summaries_itemClassification,
    {{extract_nested_value("summaries","itemName","string")}} as summaries_itemName,
    {{extract_nested_value("summaries","manufacturer","string")}} as summaries_manufacturer,
    {{extract_nested_value("summaries","modelNumber","string")}} as summaries_modelNumber,
    {{extract_nested_value("summaries","packageQuantity","integer")}} as summaries_packageQuantity,
    {{extract_nested_value("summaries","partNumber","string")}} as summaries_partNumber,
    {{extract_nested_value("summaries","size","string")}} as summaries_sizeName,
    {{extract_nested_value("summaries","style","string")}} as summaries_styleName,
    {{extract_nested_value("summaries","websiteDisplayGroup","string")}} as summaries_websiteDisplayGroup,
    {{extract_nested_value("summaries","websiteDisplayGroupName","string")}} as summaries_websiteDisplayGroupName,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
        {{unnesting("summaries")}}
        {{multi_unnesting("summaries","browseClassification")}}
        {{unnesting("salesRanks")}}
        {{multi_unnesting("salesRanks","classificationRanks")}}
        {{multi_unnesting("salesRanks","displayGroupRanks")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('CatalogItems_lookback') }},0) from {{ this }})
        {% endif %}
        qualify row_number() over (partition by {{extract_nested_value("summaries","brand","string")}},ReferenceASIN,{{extract_nested_value("summaries","modelNumber","string")}},{{extract_nested_value("summaries","marketplaceId","string")}} order by {{daton_batch_runtime()}} desc, {{daton_batch_id()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
