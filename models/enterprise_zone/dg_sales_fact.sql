{{
       config(
             materialized='table',
             tags = 'dg_sales_fact',
         )
}}


select
  a.store,
  a.storeid,
  a.date,
  a.saleid,
  a.storetype,
  a.storeparent,
  a.item,
  a.unitprice,
  a.qty,
  a.amt,
  a.unitcost,
  a.totalcost,
  a.totalprofit,
  a.margin,
  a.avg_basket_size,
  a.total_basket_size,
  a.avg_basket_value_itemized,
  b.item_number,
  b.inventory_item_id
from
  cg-gbq-p.lightspeed.v_Sales_L3Y a
left join
  cg-gbq-p.consumption_zone.cg_product_dimension b
on
  a.item = b.item_number
  where StoreType = 'Brand'