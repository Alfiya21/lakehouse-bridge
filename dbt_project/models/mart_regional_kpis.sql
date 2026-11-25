with orders as (select region, count(*) as orders, round(sum(amount), 2) as revenue from {{ ref('stg_orders') }} group by region),
     custs  as (select region, count(*) as customers, sum(case when value_tier = 'TOP_25' then 1 else 0 end) as top_tier_customers
                from {{ ref('dim_customer_value') }} group by region)
select o.region, o.orders, o.revenue, c.customers, c.top_tier_customers,
       round(o.revenue / nullif(c.customers, 0), 2) as revenue_per_customer
from orders o join custs c using (region)
