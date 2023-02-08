{% macro country(variable) %}
    case
    when {{variable}} = 'Amazon.co.mx' then 'Mexico'
    when {{variable}}  = 'Amazon.ca' then 'Canada'
    when {{variable}}  = 'Amazon.co.uk' then 'United Kingdom'
    when {{variable}}  = 'Amazon.com' then 'United States'
    when {{variable}}  = 'USD' then 'United States'
    when {{variable}}  = 'CAD' then 'Canada'
    when {{variable}}  = 'INR' then 'India'
    when {{variable}}  = 'UK' then 'United Kingdom'
    when {{variable}}  = 'US' then 'United States'
    else {{variable}}  end as store_name
{% endmacro %}