{{
       config(
             materialized='table',
             unique_key = 's_key',
             tags = 'dg_sales_fact'
         )
   }}
WITH
  light_speed_data AS (
  SELECT
    DISTINCT store,
    storeid,
    DATE(date) AS date,
    saleid,
    storetype,
    item,
    SUM(qty) qty,
    SUM(amt) amt,
  FROM
    cg-gbq-p.lightspeed.v_Sales_L3Y
  WHERE
    StoreType = 'Brand'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6 )
SELECT
  MD5(CONCAT(a.store, a.storeid, a.date, a.saleid, a.storetype, a.item, COALESCE(b.item_number,'0'), COALESCE(b.inventory_item_id,0))) AS s_key,
  a.store,
  a.storeid,
  a.date,
  a.saleid,
  a.storetype,
  a.item,
  a.qty,
  a.amt,
  sc.current_cost AS pre_standard_cost,
  COALESCE(sc.current_cost * a.qty,0) AS standard_cost,
  COALESCE(b.item_number,'0') item_number,
  COALESCE(b.inventory_item_id,0) inventory_item_id,
  CURRENT_DATETIME() AS load_date_time
FROM
  light_speed_data a
LEFT JOIN
  `cg-gbq-p.consumption_zone.cg_product_dimension` b
ON
  a.item = b.item_number
LEFT JOIN
  --`cg-gbq-p.staging_zone.standard_cost` sc 
  {{ ref('standard_cost') }} sc
ON
  a.item= sc.item_number
  AND sc.organization_code = 'CUSTOM_GOODS_CA'