{% if var('CatalogItems') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('CatalogItems_tbl_ptrn'),
exclude=var('CatalogItems_tbl_exclude_ptrn'),
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
    {{ timezone_conversion("RequeststartDate") }} as RequeststartDate,
    {{ timezone_conversion("RequestendDate") }} as RequestendDate,
    coalesce(ReferenceASIN,'N/A') as ReferenceASIN,
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
    coalesce({{extract_nested_value("summaries","brand","string")}},'N/A') as summaries_brandName,
    {{extract_nested_value("summaries","color","string")}} as summaries_colorName,
    {{extract_nested_value("summaries","itemClassification","string")}} as summaries_itemClassification,
    {{extract_nested_value("summaries","itemName","string")}} as summaries_itemName,
    {{extract_nested_value("summaries","manufacturer","string")}} as summaries_manufacturer,
    coalesce({{extract_nested_value("summaries","modelNumber","string")}},'N/A') as summaries_modelNumber,
    {{extract_nested_value("summaries","packageQuantity","integer")}} as summaries_packageQuantity,
    {{extract_nested_value("summaries","partNumber","string")}} as summaries_partNumber,
    {{extract_nested_value("summaries","size","string")}} as summaries_sizeName,
    {{extract_nested_value("summaries","style","string")}} as summaries_styleName,
    {{extract_nested_value("summaries","websiteDisplayGroup","string")}} as summaries_websiteDisplayGroup,
    {{extract_nested_value("summaries","websiteDisplayGroupName","string")}} as summaries_websiteDisplayGroupName,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
        {{unnesting("summaries")}}
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
