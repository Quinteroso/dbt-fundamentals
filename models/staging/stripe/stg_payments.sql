select 
    id as payment_id,
    orderid as order_id,
    paymentmethod,
    status,
    amount
from raw.stripe.payment

