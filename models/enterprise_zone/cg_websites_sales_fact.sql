{{
       config(
             materialized='table',
             tags = 'cg_websites_sales_fact',
         )
}}

select distinct
  a.order_summary_number,
  a.order_line_number,
  a.status,
  a.quantity,
  a.quantity_ordered,
  a.line_subtotal,
  a.distributed_order_adjustments,
  a.line_adjustments,
  a.pretax_total,
  a.product_product_code,
  a._fivetran_synced as details_fivetran_synced,
  date(b.created_date) created_date,
  b.subtotal,
  b.pretax_subtotal,
  b._fivetran_synced as header_fivetran_synced,
  b.account_account_id,
  b.account_account_name,
  c.item_number,
  coalesce(c.inventory_item_id,0) inventory_item_id,
  c.item_dsecription,
  c.item_status_code,
  c.item_type,
  sc.current_cost as pre_standard_cost,
--   case when (a.product_product_code like 'VIR%' or a.product_product_code like 'KIT%')
--   then pretax_total else 
   COALESCE(sc.current_cost * a.quantity,0) AS standard_cost,
  current_datetime() as load_date_time
from
  `cg-gbq-p.staging_zone.sales_force_order_details_view` a
inner join
  `cg-gbq-p.staging_zone.sales_force_order_header_view` b
on
  a.order_summary_number = b.order_summary_number
left join
  `cg-gbq-p.consumption_zone.cg_product_dimension` c
on
  c.item_number = a.product_product_code
left join 
  --`cg-gbq-p.staging_zone.standard_cost` sc
  {{ ref('standard_cost') }} sc
  on 
a.product_product_code= sc.item_number
and sc.organization_code = 'CUSTOM_GOODS_CA'