with wide_orders as (select * from {{ ref('wide_orders') }})

, order_lineitem_metrics as (select * from {{ ref('order_lineitem_metrics') }}) 

SELECT 
        wo.customer_key,
        wo.customer_name,
        MIN(wo.total_price) AS min_order_value,
        MAX(wo.total_price) AS max_order_value,
        AVG(wo.total_price) AS avg_order_value,
        AVG(olm.num_lineitems) AS avg_num_items_per_order
    FROM wide_orders wo
    LEFT JOIN order_lineitem_metrics olm ON wo.order_key = olm.order_key
    GROUP BY wo.customer_key, wo.customer_name
