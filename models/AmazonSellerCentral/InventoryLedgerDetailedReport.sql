-- depends_on: {{ref('ExchangeRates')}}

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
    {{set_table_name('%ledgerdetail%')}}    
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
                reportstartdate,
                reportenddate,
                reportrequesttime,
                sellingpartnerid,
                marketplacename,
                marketplaceid,
                {% if var('timezone_conversion_flag') %}
                    cast(DATETIME_ADD(cast(Date as timestamp), INTERVAL {{hr}} HOUR ) as DATE) Date,
                {% else %}
                Date,
                {% endif %}
                fnsku,
                coalesce(asin,'') as asin,
                coalesce(msku,'') as msku,
                title,
                coalesce(event_type,'') as event_type,
                coalesce(reference_id,'') as reference_id,
                coalesce(quantity,0) as quantity,
                coalesce(fulfillment_center,'') as fulfillment_center,
                coalesce(disposition,'') as disposition,
                reason,
                country,
	   	        {{daton_user_id()}},
       	        {{daton_batch_runtime()}},
                {{daton_batch_id()}},
                current_timestamp() as last_updated,
                null as run_id,
                ROW_NUMBER() OVER (PARTITION BY Date,asin, msku, fulfillment_center, event_type, reference_id, quantity, disposition order by {{daton_batch_runtime()}} desc) row_num
                from {{i}} 
                    {% if is_incremental() %}
                    {# /* -- this filter will only be applied on an incremental run */ #}
                    WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                    {% endif %}
                )
            where row_num =1 
            {% if not loop.last %} union all {% endif %}
        {% endfor %}