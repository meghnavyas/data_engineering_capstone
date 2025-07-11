with wide_lineitem as (select * from {{ ref('wide_lineitem') }})
SELECT 
        l_orderkey as order_key,
        COUNT(l_linenumber) AS num_lineitems
    FROM wide_lineitem
    GROUP BY l_orderkey