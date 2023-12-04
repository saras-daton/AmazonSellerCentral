{% macro currency_conversion( value, from_currency_code, currency) %}   

    {% if var('currency_conversion_flag') %}      
        case when {{ value }} is null then 1 else {{ value }} end as exchange_currency_rate,      
        case when {{ from_currency_code }} is null then {{ currency }} else {{ from_currency_code }} end as exchange_currency_code   
    {% else %}        
        safe_cast(1 as decimal) as exchange_currency_rate,       
        {{ currency }} as exchange_currency_code     
    {% endif %} 

{% endmacro %}