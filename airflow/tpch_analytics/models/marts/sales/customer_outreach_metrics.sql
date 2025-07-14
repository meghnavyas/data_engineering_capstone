with wide_orders as (select * from {{ ref('wide_orders') }})

, order_lineitem_metrics as (select * from {{ ref('order_lineitem_metrics') }}) 

SELECT 
        wo.customer_key,
        wo.customer_name,
        MIN(wo.o_totalprice) AS min_order_value,
        MAX(wo.o_totalprice) AS max_order_value,
        AVG(wo.o_totalprice) AS avg_order_value,
        AVG(olm.num_lineitems) AS avg_num_items_per_order
    FROM wide_orders wo
    LEFT JOIN order_lineitem_metrics olm ON wo.o_orderkey = olm.order_key
    GROUP BY wo.customer_key, wo.customer_name
