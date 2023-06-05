{{
       config(
             materialized='view',
             tags = 'cg_report_sales_order_view'
         )
   }}


WITH union_all_cte AS (
  SELECT
  DATE(request_ship_date) date_key,
  inventory_item_id,
  cc.customer_account_id,
  order_number as source_number,
  sum (extended_amount) order_amount,
  'OPEN_ORDERS' as source_key
FROM
 -- cg-gbq-p.enterprise_zone.cg_sales_orders_inc_fact co
  {{ ref('cg_sales_orders_inc_fact') }} co
JOIN
  -- cg-gbq-p.consumption_zone.cg_customer_class cc
  {{ ref('cg_customer_class') }} cc
ON
  co.account_name = cc.account_name
WHERE
  co.line_status='AWAIT_SHIP'
GROUP BY
  DATE(request_ship_date),
  inventory_item_id,
  cc.customer_account_id,
  order_number
UNION ALL
SELECT
  DATE(REQUEST_SHIP_DATE) date_key,
  inventory_item_id,
  cc.CUSTOMER_ACCOUNT_ID,
  order_number as source_number,
  SUM (extended_amount) order_amount,
  'OPEN_ORDERS_SHIPPED' as source_key
FROM
 -- cg-gbq-p.enterprise_zone.cg_sales_orders_inc_fact co
  {{ ref('cg_sales_orders_inc_fact') }} co
JOIN
  --cg-gbq-p.consumption_zone.cg_customer_class cc
  {{ ref('cg_customer_class') }} cc
ON
  co.account_name = cc.account_name
WHERE
  co.line_status='SHIPPED'
GROUP BY
  DATE(REQUEST_SHIP_DATE),
  inventory_item_id,
  cc.CUSTOMER_ACCOUNT_ID,
  order_number
UNION ALL
SELECT
  DATE(REQUEST_SHIP_DATE) date_key,
  inventory_item_id,
  cc.customer_account_id,
  order_number as source_number,
  SUM (extended_amount) order_amount,
  'OPEN_ORDERS_AWAIT_BILLING' as source_key
FROM
--cg-gbq-p.enterprise_zone.cg_sales_orders_inc_fact co
  {{ ref('cg_sales_orders_inc_fact') }} co
JOIN
--cg-gbq-p.consumption_zone.cg_customer_class cc
 {{ ref('cg_customer_class') }} cc
ON
  co.account_name = cc.account_name
WHERE
  co.line_status='AWAIT_BILLING'
GROUP BY
  DATE(REQUEST_SHIP_DATE),
  inventory_item_id,
  cc.customer_account_id,
  order_number
UNION ALL
  SELECT
  created_date AS date_key,
  coalesce(inventory_item_id,0) inventory_item_id,
  1424 AS customer_account_id,
  --instead of cross join hard coded US WEBSITES customer_id present in cg_customer_class table
  order_summary_number AS source_number,
  case when substring(cast(created_date as string),1,7) = substring(cast(current_date as string),1,7) 
  then
  ROUND(sum(Pretax_Total)/EXTRACT(DAY FROM current_date - 1 ),2) *
(extract(day from  last_day(current_date)) - extract(day FROM current_date - 1 )) else 0 end 
 order_amount,
  'WEBSITES DATA' AS source_key
FROM
--`cg-gbq-p.enterprise_zone.cg_websites_sales_fact`
  {{ ref('cg_websites_sales_fact') }}
GROUP BY
  created_date,
  coalesce(inventory_item_id,0),
  order_summary_number
 union all
 SELECT
  date as date_key,
  coalesce(inventory_item_id,0) inventory_item_id,
  1423 as customer_account_id,
  --instead of cross join hard coded OWNED STORE SALES customer_id present in cg_customer_class table
  cast(saleid as string) as source_number,
  ROUND(case when substring(cast(date as string),1,7) = substring(cast(current_date as string),1,7) 
  then
  sum(amt)/EXTRACT(DAY FROM current_date - 1 ) *
(extract(day from  last_day(current_date)) - extract(day FROM current_date - 1 )) else 0 end ,2)
 order_amount,
  'LIGHT SPEED DATA' as source_key

 FROM
--`cg-gbq-p.enterprise_zone.dg_sales_fact` 
{{ ref('dg_sales_fact') }}
group by 
date,
coalesce(inventory_item_id,0),
saleid
  )
select 
md5(concat(date_key,inventory_item_id,customer_account_id,source_number,source_key)) as s_key,
date_key,
inventory_item_id,
customer_account_id,
source_number,
sum(order_amount) order_amount,
source_key
from union_all_cte
group by date_key,
inventory_item_id,
customer_account_id,
source_number,
source_key