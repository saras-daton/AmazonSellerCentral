    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        partition_by = { 'field': 'date', 'data_type': 'date' },
        cluster_by = ['from_currency_code','to_currency_code'], 
        unique_key = ['date','from_currency_code','to_currency_code'])}}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT MAX({{daton_batch_runtime()}}) - 2592000000 FROM {{ this }}
    {% endset %} 

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}

    {% set table_name_query %}
    {{set_table_name('%exchangerates')}}    
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
        SELECT * {{exclude()}} (row_num)
        From (
            select 
            date, 
            from_currency_code, 
            to_currency_code, 
            value,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
            DENSE_RANK() OVER (PARTITION BY date, from_currency_code, to_currency_code order by {{daton_batch_runtime()}} desc) as row_num
            from {{i}}  
             {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                qualify {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}                    
            )
            where row_num =1  
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
