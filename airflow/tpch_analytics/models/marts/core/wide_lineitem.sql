With fct_lineitem as (
Select * 
        from 
{{ ref('fct_lineitem') }})

select order_key
    , line_number
from fct_lineitem
