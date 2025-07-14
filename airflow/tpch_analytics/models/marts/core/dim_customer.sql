with customer as (
    select *
    from {{ ref('stg_customer') }}
),

nation as (
    select *
    from {{ ref('stg_nation') }}
),

region as (
    select *
    from {{ ref('stg_region') }}
)

SELECT 
        c.customer_key,
        c.customer_name,
        n.nation_name,
        n.nation_comment,
        r.region_name,
        r.region_comment
    FROM customer c
    LEFT JOIN nation n ON c.nation_key = n.nation_key
    LEFT JOIN region r ON n.region_key = r.region_key
