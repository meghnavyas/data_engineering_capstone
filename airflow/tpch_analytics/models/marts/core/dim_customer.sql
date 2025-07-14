with customer as (
    select *
    from {{ ref('stg_customer') }}
),

nation as (
    select *
    from {{ source('source', 'nation') }}
),

region as (
    select *
    from {{ source('source', 'region') }}
)

SELECT 
        c.customer_key,
        c.customer_name,
        n_name AS nation_name,
        n_comment AS nation_comment,
        r_name AS region_name,
        r_comment AS region_comment
    FROM customer c
    LEFT JOIN nation n ON c.nation_key = n_nationkey
    LEFT JOIN region r ON n_regionkey = r_regionkey
