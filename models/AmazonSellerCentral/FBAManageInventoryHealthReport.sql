-- depends_on: {{ref('ExchangeRates')}}
    {% if var('table_partition_flag') %}
    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        partition_by = { 'field': 'snapshot_date', 'data_type': 'date' },
        cluster_by = ['asin','sku'], 
        unique_key = ['snapshot_date','asin','sku'])}}
    {% else %}
    {{config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        unique_key = ['snapshot_date','asin','sku'])}}
    {% endif %}

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
    {{set_table_name('%fbamanageinventoryhealthreport')}}    
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
            '{{id}}' as brand,
            '{{store}}' as store,
            CAST(ReportstartDate as timestamp) ReportstartDate,
            CAST(ReportendDate as timestamp) ReportendDate,
            cast(ReportRequestTime as Date)ReportRequestTime,
            sellingPartnerId,
            marketplaceName,
            marketplaceId,
            CAST(snapshot_date as DATE) snapshot_date,
            coalesce(sku,'') as sku,
            fnsku,
            coalesce(asin,'') as asin,
            product_name,
            condition,
            available,
            pending_removal_quantity,
            inv_age_0_to_90_days,
            inv_age_91_to_180_days,
            inv_age_181_to_270_days,
            inv_age_271_to_365_days,
            inv_age_365_plus_days,
            currency,
            qty_to_be_charged_ltsf_6_mo,
            projected_ltsf_6_mo,
            qty_to_be_charged_ltsf_12_mo,
            estimated_ltsf_next_charge,
            units_shipped_t7,
            units_shipped_t30,
            units_shipped_t60,
            units_shipped_t90,
            alert,
            your_price,
            sales_price,
            lowest_price_new_plus_shipping,
            lowest_price_used,
            recommended_action,
            healthy_inventory_level,
            recommended_sales_price,
            recommended_sale_duration_days,
            recommended_removal_quantity,
            estimated_cost_savings_of_recommended_actions,
            sell_through,
            item_volume,
            volume_unit_measurement,
            storage_type,
            storage_volume,
            marketplace,
            product_group,
            sales_rank,
            days_of_supply,
            estimated_excess_quantity,
            weeks_of_cover_t30,
            weeks_of_cover_t90,
            featuredoffer_price,
            sales_shipped_last_7_days,
            sales_shipped_last_30_days,
            sales_shipped_last_60_days,
            sales_shipped_last_90_days,
            inv_age_0_to_30_days,
            inv_age_31_to_60_days,
            inv_age_61_to_90_days,
            inv_age_181_to_330_days,
            inv_age_331_to_365_days,
            estimated_storage_cost_next_month,
            inbound_quantity,
            inbound_working,
            inbound_shipped,
            inbound_received,
            no_sale_last_6_months,
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                cast(null as string) as exchange_currency_code, 
            {% endif %}
	        a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(snapshot_date as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id,
            {% else %}
                cast(snapshot_date as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id,
            {% endif %}
            DENSE_RANK() OVER (PARTITION BY snapshot_date, asin,
            sku order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.ReportRequestTime) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
            )
        where row_num =1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}