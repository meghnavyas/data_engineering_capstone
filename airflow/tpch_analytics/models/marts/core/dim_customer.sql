with customer as (
    select *
    from {{ source('source', 'customer') }}
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
        c.*,
        n_name AS nation_name,
        n_comment AS nation_comment,
        r_name AS region_name,
        r_comment AS region_comment
    FROM customer c
    LEFT JOIN nation n ON c_nationkey = n_nationkey
    LEFT JOIN region r ON n_regionkey = r_regionkey
