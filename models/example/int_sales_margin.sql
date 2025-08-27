with
order_items as (
  select * from {{ ref('stg_order_items') }}
),
products as (
  select * from {{ ref('stg_products') }}
),
orders as (
  -- só o essencial para esta etapa
  select orders_id, date_date
  from {{ ref('stg_orders') }}
)

select
  oi.orders_id,
  o.date_date,
  oi.product_id,

  -- medidas base, com salvaguarda de nulos
  coalesce(oi.quantity, 0)            as quantity,
  coalesce(oi.unit_price, 0)          as unit_price,
  coalesce(p.purchase_price, 0)       as purchase_price,

  -- cálculos pedidos
  coalesce(oi.quantity, 0) * coalesce(oi.unit_price, 0)        as revenue,
  coalesce(oi.quantity, 0) * coalesce(p.purchase_price, 0)     as purchase_cost,
  (coalesce(oi.quantity, 0) * coalesce(oi.unit_price, 0))
  - (coalesce(oi.quantity, 0) * coalesce(p.purchase_price, 0)) as margin

from order_items oi
left join products p on oi.product_id = p.product_id
left join orders   o on oi.orders_id  = o.orders_id
