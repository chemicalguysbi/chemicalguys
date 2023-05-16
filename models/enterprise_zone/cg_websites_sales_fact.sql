{{
       config(
             materialized='table',
             tags = 'cg_websites_sales_fact',
         )
}}

select
  a.order_summary_number,
  a.order_line_number,
  a.status,
  a.quantity_fulfilled,
  a.quantity_returned,
  a.quantity,
  a.quantity_ordered,
  a.unit_price,
  a.gross_unit_price,
  a.gross_revenue,
  a.line_subtotal,
  a.tax as order_details_tax,
  a.distributed_order_adjustments,
  a.line_adjustments,
  a.pretax_total,
  a.product_product_code,
  a._fivetran_synced as details_fivetran_synced,
  b.billing_email_address,
  date(b.created_date) created_date,
  b.subtotal,
  b.shipping,
  b.pretax_subtotal,
  b.tax,
  b.order_summary_total,
  b.created_by,
  b._fivetran_synced,
  b.account_account_id,
  b.account_account_name,
  c.item_number,
  c.inventory_item_id,
  c.item_dsecription,
  c.item_status_code,
  c.item_type,
  c.planner_code,
  c.uom_code,
from
  `cg-gbq-p.staging_zone.sales_force_order_details_view` a
inner join
  `cg-gbq-p.staging_zone.sales_force_order_header_view` b
on
  a.order_summary_number = b.order_summary_number
left join
  cg-gbq-p.consumption_zone.cg_product_dimension c
on
  c.item_number = a.product_product_code