{{
       config(
             materialized='table',
             tags = 'dg_sales_fact',
         )
}}


select distinct
  a.store,
  a.storeid,
  date(a.date) as date,
  a.saleid,
  a.storetype,
  a.storeparent,
  a.item,
  a.qty,
  a.amt,
  b.item_number,
  coalesce(b.inventory_item_id,0) inventory_item_id,
  current_datetime() as load_date_time
from
  cg-gbq-p.lightspeed.v_Sales_L3Y a
left join
  cg-gbq-p.consumption_zone.cg_product_dimension b
on
  a.item = b.item_number
  where StoreType = 'Brand'