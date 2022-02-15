with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
payment_amount as (
    select order_id, sum(amount) as total_amount
    from payments
    where status='success'
    group by 1
)
select
    o.order_id,
    o.customer_id,
    order_date,
    coalesce(pa.total_amount, 0) as amount
from payment_amount pa
join orders o
  on pa.order_id = o.order_id