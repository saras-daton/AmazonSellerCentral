
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        cluster_by = ['ReferenceASIN'],
        unique_key = ['brandName','ReferenceASIN','modelNumber'])}}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}

    {% set table_name_query %}
    {{set_table_name('%catalogitemssummary')}}    
    {% endset %}   


    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}



    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}

    {% for i in results_list %}
        {% if var('brand_consolidation_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brand_name_position')] %}
        {% else %}
            {% set brand = var('brand_name') %}
        {% endif %}

        {% if var('store_consolidation_flag') %}
            {% set store =i.split('.')[2].split('_')[var('store_name_position')] %}
        {% else %}
            {% set store = var('store') %}
        {% endif %}

        SELECT *  {{exclude()}} (row_num)
        From (
            select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            CAST(RequeststartDate as timestamp) RequeststartDate,
            CAST(RequestendDate as timestamp) RequestendDate,
            coalesce(ReferenceASIN,'') as ReferenceASIN,
            marketplaceId,
            brandName,
            browseNode,
            colorName,
            itemName,
            manufacturer,
            modelNumber,
            sizeName,
            styleName,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
            {% if var('timezone_conversion_flag') %}
                DATETIME_ADD({{from_epoch_milliseconds()}}, INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id,
            {% else %}
                {{from_epoch_milliseconds()}} effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id,
            {% endif %}
            ROW_NUMBER() OVER (PARTITION BY brandName,ReferenceASIN,modelNumber order by {{daton_batch_runtime()}} desc, {{daton_batch_id()}} desc) row_num
    	    from {{i}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}   
             )
          where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}



