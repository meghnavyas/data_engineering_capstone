with fct_orders as (select * from {{ ref('fct_orders') }}),
dim_customer as (select * from {{ ref('dim_customer') }})
SELECT 
        f.order_key,
        f.total_price,
        d.customer_name
        , d.customer_key
    FROM fct_orders f
    LEFT JOIN dim_customer d ON f.customer_key = d.customer_key
