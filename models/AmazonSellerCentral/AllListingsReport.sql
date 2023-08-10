{% if var('AllListingsreport') %}
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
    {{set_table_name('%alllistingsreport')}}    
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
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportstartDate") }} as {{ dbt.type_timestamp() }}) as ReportstartDate,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportendDate") }} as {{ dbt.type_timestamp() }}) as ReportendDate,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ReportRequestTime") }} as {{ dbt.type_timestamp() }}) as ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            item_name,
            item_description,
            listing_id,
            seller_sku,
            price,
            quantity,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="open_date") }} as {{ dbt.type_timestamp() }}) as open_date,
            image_url,
            item_is_marketplace,
            product_id_type,
            zshop_shipping_fee,
            item_note,
            item_condition,
            zshop_category1,
            zshop_browse_path,
            zshop_storefront_feature,
            asin1,
            asin2,
            asin3,
            will_ship_internationally,
            expedited_shipping,
            zshop_boldface,
            product_id,
            bid_for_featured_placement,
            add_delete,
            pending_quantity,
            fulfillment_channel,
            optional_payment_type_exclusion,
            merchant_shipping_group,
            status,
            maximum_retail_price,
            scheduled_delivery_sku_set,
            standard_price_point,
            ProductTaxCode,
            minimum_seller_allowed_price,
            maximum_seller_allowed_price,
	        {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
            from {{i}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}         
            qualify dense_rank() over (partition by seller_sku, listing_id order by {{daton_batch_runtime()}} desc) = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
