with wide_lineitem as (select * from {{ ref('wide_lineitem') }})
SELECT 
       order_key,
       COUNT(line_number) AS num_lineitems
    FROM wide_lineitem
    GROUP BY order_key
