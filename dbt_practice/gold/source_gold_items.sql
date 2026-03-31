with dedep_query as (
    select
        *,
        row_number() over (partition by id order by updateDate desc) as deduplication_id
    from {{ source('source', 'items') }}
)

select 
    id,
    name,
    category,
    updateDate
from dedep_query
where deduplication_id = 1
