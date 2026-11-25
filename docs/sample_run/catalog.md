# Data catalog

## Table gold_customer_ltv
Rows: 2000. Upstream: silver_orders, silver_customers. PII columns: none.

- gold_customer_ltv.customer_id (BIGINT).
- gold_customer_ltv.region (VARCHAR).
- gold_customer_ltv.segment (VARCHAR).
- gold_customer_ltv.order_count (BIGINT).
- gold_customer_ltv.total_revenue (DOUBLE). Glossary: Customer Lifetime Value.
- gold_customer_ltv.first_order_date (DATE).
- gold_customer_ltv.last_order_date (DATE).
- gold_customer_ltv.revenue_rank_in_region (INTEGER).
- gold_customer_ltv.ltv_quartile (INTEGER). Glossary: Customer Lifetime Value.

## Table gold_daily_revenue
Rows: 1620. Upstream: silver_orders. PII columns: none.

- gold_daily_revenue.order_date (DATE).
- gold_daily_revenue.region (VARCHAR).
- gold_daily_revenue.orders (BIGINT).
- gold_daily_revenue.revenue (DOUBLE). Glossary: Revenue.
- gold_daily_revenue.avg_order_value (DOUBLE). Glossary: Average Order Value.

## Table gold_payments_hourly
Rows: 8496. Upstream: silver_payments. PII columns: none.

- gold_payments_hourly.hour_ts (TIMESTAMP).
- gold_payments_hourly.method (VARCHAR).
- gold_payments_hourly.events (BIGINT).
- gold_payments_hourly.successes (BIGINT). Glossary: Payment Success Rate.
- gold_payments_hourly.success_amount (DOUBLE).

## Table quarantine_orders
Rows: 398. Upstream: bronze/orders. PII columns: none.

- quarantine_orders.customer_id (BIGINT).
- quarantine_orders.order_id (BIGINT).
- quarantine_orders.order_ts (TIMESTAMP).
- quarantine_orders.amount (DOUBLE).
- quarantine_orders.status (VARCHAR).
- quarantine_orders.currency (VARCHAR).
- quarantine_orders.region (VARCHAR).
- quarantine_orders.reason (VARCHAR). Glossary: Quarantine.

## Table silver_customers
Rows: 2000. Upstream: bronze/customers. PII columns: full_name, email, phone.

- silver_customers.customer_id (BIGINT).
- silver_customers.full_name (VARCHAR). PII=PERSON_NAME. Glossary: PII.
- silver_customers.email (VARCHAR). PII=EMAIL. Glossary: PII.
- silver_customers.phone (VARCHAR). PII=PHONE. Glossary: PII.
- silver_customers.region (VARCHAR).
- silver_customers.segment (VARCHAR).
- silver_customers.signup_date (DATE).

## Table silver_orders
Rows: 19602. Upstream: bronze/orders, silver_customers. PII columns: none.

- silver_orders.customer_id (BIGINT).
- silver_orders.order_id (BIGINT).
- silver_orders.order_ts (TIMESTAMP).
- silver_orders.amount (DOUBLE).
- silver_orders.status (VARCHAR).
- silver_orders.currency (VARCHAR).
- silver_orders.region (VARCHAR).
- silver_orders.order_date (DATE).
- silver_orders.order_month (VARCHAR).

## Table silver_payments
Rows: 10000. Upstream: landing/stream/*.json. PII columns: none.

- silver_payments.event_id (VARCHAR).
- silver_payments.order_id (BIGINT).
- silver_payments.event_ts (TIMESTAMP).
- silver_payments.method (VARCHAR).
- silver_payments.amount (DOUBLE).
- silver_payments.status (VARCHAR).

## Column lineage
- gold_daily_revenue.revenue is derived from silver_orders.amount, silver_orders.status, silver_orders.order_date.
- gold_customer_ltv.total_revenue is derived from silver_orders.amount, silver_orders.customer_id, silver_customers.customer_id.
- gold_payments_hourly.success_amount is derived from silver_payments.amount, silver_payments.status.

## Business glossary
### Revenue
Sum of order amount in INR for orders whose status is not CANCELLED.

### Average Order Value
Revenue divided by number of non-cancelled orders for the same grain.

### Customer Lifetime Value
Total non-cancelled revenue of a customer since signup; LTV quartile 1 is the top 25% of customers.

### Quarantine
Orders rejected by silver validation (non-positive amount or unknown customer) with a machine-readable reason. They are excluded from all metrics.

### Payment Success Rate
successes divided by events in gold_payments_hourly for the same hour and method.

### PII
Personally identifiable information: full_name, email and phone. Masked for every role except admin.
