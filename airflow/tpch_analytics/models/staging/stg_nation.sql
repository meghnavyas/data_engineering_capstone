with nation as (
    select *
    from {{ source('source', 'nation') }}
)

SELECT 
	n_nationkey as nation_key,
        n_name AS nation_name,
        n_comment AS nation_comment,
	n_regionkey as region_key
    FROM nation
