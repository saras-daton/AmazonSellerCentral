{% if var('InventoryLedgerDetailedReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ref('ExchangeRates')}}
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
    {{set_table_name('%ledgerdetail%')}}    
    {% endset %}  


    {% set results = run_query(table_name_query) %}
    {% if execute %}
        {# Return the first column #}
        {% set results_list = results.columns[0].values() %}
        {% set tables_lowercase_list = results.columns[1].values() %}
    {% else %}
        {% set results_list = [] %}
        {% set tables_lowercase_list = [] %}
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

            select * from (
                select 
                '{{brand}}' as brand,
                '{{store}}' as store,
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="reportstartdate") }} as {{ dbt.type_timestamp() }}) as reportstartdate,
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="reportenddate") }} as {{ dbt.type_timestamp() }}) as reportenddate,
                cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="reportrequesttime") }} as {{ dbt.type_timestamp() }}) as reportrequesttime,
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
                    where {{daton_batch_runtime()}}  >= {{max_loaded}}
                    {% endif %}
                )
            qualify row_number() over (partition by Date,asin, msku, fulfillment_center, event_type, reference_id, quantity, disposition, marketplaceid order by _daton_batch_runtime desc) = 1
            {% if not loop.last %} union all {% endif %}
        {% endfor %}