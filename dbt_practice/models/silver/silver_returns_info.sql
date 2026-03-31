with returns as (
    select sales_id, 
    date_sk,
    store_sk,
    returned_qty,
    return_reason, 
    refund_amount
from {{ ref('bronze_returns') }}
),
store as (
    select store_sk,
    store_code,
    store_name,
    city,
    region,
    country
from {{ ref('bronze_store') }}
),
date_values as (
    select date_sk,
    date,
    day,
    month,
    quarter,
    year
from {{ ref('bronze_date') }}
),
joined_query as (
    select
    store_name,
    store_code,
    returned_qty,
    return_reason, 
    refund_amount,
    city,
    region,
    country,
    date,
    day,
    month,
    quarter,
    year
from returns r
join store s on r.store_sk = s.store_sk
join date_values d on r.date_sk = d.date_sk
)

-- what is the common reason for returns in each region?
select region, 
    store_code,
    return_reason, 
    sum(returned_qty) as total_returned_qty,
    country,
    month
from joined_query
group by region, return_reason, store_code, country, month
order by return_reason, total_returned_qty desc
