{% if var('SuppressedListingsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('SuppressedListingsReport_tbl_ptrn'),
exclude=var('SuppressedListingsReport_tbl_exclude_ptrn'),
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
            {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
            {{ timezone_conversion("ReportendDate") }} as ReportendDate,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            Status,
            Reason,
            SKU,
            ASIN,
            Product_name,
            Condition,
            {% if target.type == 'snowflake'%}
                TO_DATE(Status_Change_Date, 'MMM DD, YYYY') AS Status_Change_Date,
             {% else %}  
                PARSE_DATE('%b %d, %Y',Status_Change_Date) AS Status_Change_Date,
            {% endif %}    
            Issue_Description,
            a.{{daton_user_id()}} as _daton_user_id,
            a.{{daton_batch_runtime()}} as _daton_batch_runtime,
            a.{{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
            from {{i}}  a 
            
                {% if is_incremental() %}
                    {# /* -- this filter will only be applied on an incremental run */ #}
                    where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('FlatFileReturnsReportByReturnDate_lookback') }},0) from {{ this }})
                {% endif %}  
        
        qualify dense_rank() over (partition by ASIN, SKU, a.Status_Change_Date,a.Status order by _daton_batch_runtime desc) = 1
    
    
    {% if not loop.last %} union all {% endif %}
{% endfor %}