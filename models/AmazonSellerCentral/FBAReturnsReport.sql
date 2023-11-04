{% if var('FBAReturnsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('FBAReturnsReport_tbl_ptrn'),
exclude=var('FBAReturnsReport_tbl_exclude_ptrn'),
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

    select *, row_number() over (partition by date(return_date), asin, sku, order_id, fnsku, license_plate_number, fulfillment_center_id order by _daton_batch_runtime desc) as _seq_id 
    from (
        select 
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
        {{ timezone_conversion("ReportendDate") }} as ReportendDate,
        {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
        sellingPartnerId,
        marketplaceName,
        marketplaceId,
        cast(return_date as DATE) as return_date,
        coalesce(order_id,'N/A') as order_id,
        coalesce(sku,'N/A') as sku,
        coalesce(asin,'N/A') as asin,
        coalesce(fnsku,'N/A') as fnsku,
        product_name,
        quantity,
        coalesce(fulfillment_center_id,'N/A') as fulfillment_center_id,
        detailed_disposition,
        reason,
        status,
        coalesce(license_plate_number,'N/A') as license_plate_number,
        customer_comments,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}    
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FBAReturnsReport_lookback') }},0) from {{ this }})
            {% endif %}
        qualify dense_Rank() over (partition by date(return_date), asin, sku, order_id, fnsku, license_plate_number, fulfillment_center_id, marketplaceId order by {{daton_batch_runtime()}} desc) = 1
        ) 
    {% if not loop.last %} union all {% endif %}
{% endfor %}