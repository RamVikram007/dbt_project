with sales as (
    select 
        sales_id, 
        product_sk,
        customer_sk,
        {{ multiply('unit_price', 'quantity') }} as calculated_gross_amount,
        gross_amount,
        payment_method
    from {{ ref('bronze_sales') }}
),
product as (
    select 
        product_sk,
        category
    from {{ ref('bronze_product') }}
),
customer as (
    select 
        customer_sk,
        gender
    from {{ ref('bronze_customer') }}
),
joined_query as (
select 
    sales.sales_id,
    sales.gross_amount,
    sales.payment_method,
    product.category,
    customer.gender
from sales
JOIN product
    ON sales.product_sk = product.product_sk
JOIN customer
    ON sales.customer_sk = customer.customer_sk
)

select
    category,
    gender,
    sum(gross_amount) as total_gross_amount
from joined_query
group by category, gender
order by total_gross_amount desc