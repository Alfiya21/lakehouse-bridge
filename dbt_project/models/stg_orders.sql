select order_id, customer_id, region, order_date, status, amount
from {{ source('lake', 'silver_orders') }}
where status <> 'CANCELLED'
