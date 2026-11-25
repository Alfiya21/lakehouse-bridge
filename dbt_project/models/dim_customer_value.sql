-- Customer dimension for analytics. Deliberately excludes PII columns (full_name, email, phone).
select c.customer_id, c.region, c.segment, c.signup_date,
       l.order_count, l.total_revenue, l.ltv_quartile,
       case when l.ltv_quartile = 1 then 'TOP_25' else 'OTHER' end as value_tier
from {{ source('lake', 'silver_customers') }} c
left join {{ source('lake', 'gold_customer_ltv') }} l using (customer_id)
