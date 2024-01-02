{% if var('FlatFileFeedbackReports') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
{% endif %}


{% set result =set_table_name("FlatFileFeedbackReports_tbl_ptrn","FlatFileFeedbackReports_tbl_exclude_ptrn") %}

{% for i in result %}

       select 
        {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,            {{ timezone_conversion("ReportstartDate") }} as ReportstartDate,
            {{ timezone_conversion("ReportendDate") }} as ReportendDate,
            {{ timezone_conversion("ReportRequestTime") }} as ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
           --Below jinja will use two different date conversion formats based on  target(database) type
            {% if target.type == 'snowflake'%}
                TO_DATE(Date, 'MM/DD/YY') AS Date,
             {% else %}  
                PARSE_DATE('%m/%d/%y', date) AS Date,
            {% endif %}
            Rating,
            Comments,
            Response,
            Order_ID,
            Rater_Email,
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
        
        qualify dense_rank() over (partition by Order_ID, a.Date order by _daton_batch_runtime desc) = 1
    
    
    {% if not loop.last %} union all {% endif %}
{% endfor %}