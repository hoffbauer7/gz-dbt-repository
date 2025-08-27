with sales_margin as (
  select * from {{ ref('int_sales_margin') }}
)

select
  orders_id,
  -- se todas as linhas do pedido têm a mesma data, MIN/MAX são equivalentes
  min(date_date) as date_date,

  sum(revenue)        as revenue,
  sum(quantity)       as quantity,
  sum(purchase_cost)  as purchase_cost,
  sum(margin)         as margin

from sales_margin
group by orders_id
