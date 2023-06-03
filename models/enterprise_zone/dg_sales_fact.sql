  {{
       config(
             materialized='table',
             unique_key = 's_key',
             tags = 'dg_sales_fact',
         )
}}

with light_speed_data as (
  select distinct
  store,
  storeid,
  date(date) as date,
  saleid,
  storetype,
  item,
  sum(qty) qty,
  sum(amt) amt,
  from 
  cg-gbq-p.lightspeed.v_Sales_L3Y 
  where StoreType = 'Brand'
  group by 1,2,3,4,5,6
)

select md5(concat(a.store,
  a.storeid,
  a.date,
  a.saleid,
  a.storetype,
  a.item,
  coalesce(b.item_number,'0'),
  coalesce(b.inventory_item_id,0))) as s_key,
  a.store,
  a.storeid,
  a.date,
  a.saleid,
  a.storetype,
  a.item,
  a.qty,
  a.amt,
  sc.current_cost as pre_standard_cost,
  COALESCE(sc.current_cost * a.qty,0) AS standard_cost,
  coalesce(b.item_number,'0') item_number,
  coalesce(b.inventory_item_id,0) inventory_item_id,
  current_datetime() as load_date_time
  from
  light_speed_data a
left join
  `cg-gbq-p.consumption_zone.cg_product_dimension` b
on
  a.item = b.item_number
  left join 
  --`cg-gbq-p.staging_zone.standard_cost` sc
  {{ ref('standard_cost') }} sc
  on 
a.item= sc.item_number
and sc.organization_code = 'CUSTOM_GOODS_CA'