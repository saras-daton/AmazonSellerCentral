{% if var('InventoryLedgerDetailedReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('InventoryLedgerDetailedReport_tbl_ptrn'),
exclude=var('InventoryLedgerDetailedReport_tbl_exclude_ptrn'),
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

    select *
    from (
        select 
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        {{ timezone_conversion("reportstartdate") }} as ReportreportstartdatestartDate,
        {{ timezone_conversion("reportenddate") }} as reportenddate,
        {{ timezone_conversion("reportrequesttime") }} as reportrequesttime,
        sellingpartnerid,
        marketplacename,
        marketplaceid,
        cast(Date as date) as Date,
        fnsku,
        coalesce(asin,'N/A') as asin,
        coalesce(msku,'N/A') as msku,
        title,
        coalesce(event_type,'N/A') as event_type,
        coalesce(reference_id,'N/A') as reference_id,
        coalesce(quantity,0) as quantity,
        coalesce(fulfillment_center,'N/A') as fulfillment_center,
        coalesce(disposition,'N/A') as disposition,
        reason,
        country,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} 
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('InventoryLedgerDetailedReport_lookback') }},0) from {{ this }})
            {% endif %}  
        )
    qualify row_number() over (partition by Date,asin, msku, fulfillment_center, event_type, reference_id, quantity, disposition, marketplaceid order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}