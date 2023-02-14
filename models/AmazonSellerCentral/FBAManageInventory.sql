
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
    {{set_table_name('%fbamanageinventory')}}    
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
            CAST(ReportstartDate as DATE) ReportstartDate,
            ReportendDate,
            ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            coalesce(sku,'') as sku,
            fnsku,
            asin,
            product_name,
            condition,
            your_price,
            mfn_listing_exists,
            mfn_fulfillable_quantity,
            afn_listing_exists,
            afn_warehouse_quantity,
            afn_fulfillable_quantity,
            afn_fulfillable_quantity_local,
            afn_fulfillable_quantity_remote,
            afn_unsellable_quantity,
            afn_reserved_quantity,
            afn_total_quantity,
            per_unit_volume,
            afn_inbound_working_quantity,
            afn_inbound_shipped_quantity,
            afn_inbound_receiving_quantity,
            afn_researching_quantity,
            afn_reserved_future_supply,
            afn_future_supply_buyable,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
            current_timestamp() as last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as run_id,
            DENSE_RANK() OVER (PARTITION BY date(ReportstartDate),
            sku order by {{daton_batch_runtime()}} desc) row_num
            from {{i}}   
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %} 
            )
        where row_num =1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}

