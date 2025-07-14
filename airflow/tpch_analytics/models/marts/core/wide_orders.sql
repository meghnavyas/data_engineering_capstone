with fct_orders as (select * from {{ ref('fct_orders') }}),
dim_customer as (select * from {{ ref('dim_customer') }})
SELECT 
        f.*,
        d.customer_name
        , d.customer_key
    FROM fct_orders f
    LEFT JOIN dim_customer d ON f.o_custkey = d.customer_key
