{{
       config(
             materialized='view',
             tags = 'cg_report_pace_sales_view'
         )
   }}

WITH union_all_cte AS (
  SELECT
  DATE(request_ship_date) date_key,
  inventory_item_id,
  cc.customer_account_id,
  order_number source_number,
  sum (extended_amount) order_amount,
  0 invoice_amount,
  0 shipped_amount,
  0 awaiting_billing_amount,
  0 planned_budget_amount,
  0 incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'OPEN_ORDERS' as source_key
FROM
  -- cg-gbq-p.enterprise_zone.cg_sales_orders_inc_fact co
  {{ ref('cg_sales_orders_inc_fact') }} co
JOIN
  -- cg-gbq-p.consumption_zone.cg_customer_class cc
  {{ ref('cg_dim_customer_class') }} cc
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
  DATE(inv_date) date_key,
  COALESCE(inventory_item_id,0) inventory_item_id,
  cust_account_id customer_account_id,
  inv_number source_number,
  0 order_amount,
  SUM(inv_total) invoice_amount,
  0 shipped_amount,
  0 awaiting_billing_amount,
  0 planned_budget_amount,
  0 incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'INVOICE_FACT' as source_key
FROM
  -- cg-gbq-p.enterprise_zone.cg_invoice_final_fact
  {{ ref('cg_invoice_final_fact') }}
WHERE
  complete_flag='Y'
GROUP BY
  DATE(inv_date),
  COALESCE(inventory_item_id,0),
  cust_account_id,
  inv_number
UNION ALL
SELECT
  DATE(inv_date) date_key,
  COALESCE(inventory_itemid,0) inventory_item_id,
  cust_account_id customer_account_id,
  inv_number source_number,
  0 order_amount,
  0 invoice_amount,
  0 shipped_amount,
  0 awaiting_billing_amount,
  0 planned_budget_amount,
  0 incomplete_inv_amount,
  SUM(standard_cost) standard_cost,
  SUM(quantity) invoiced_quantity,
  'INVOICE_COST_FACT' as source_key
FROM
  -- cg-gbq-p.`enterprise_zone.cg_invoice_target_cost`
  {{ ref('cg_invoice_target_cost') }}
WHERE
  complete_flag='Y'
GROUP BY
  DATE(inv_date),
  COALESCE(inventory_itemid,0),
  cust_account_id,
  inv_number
UNION ALL
SELECT
  DATE(inv_date) date_key,
  COALESCE(inventory_item_id,0) inventory_item_id,
  cust_account_id customer_account_id,
  inv_number source_number,
  0 order_amount,
  0 invoice_amount,
  0 shipped_amount,
  0 awaiting_billing_amount,
  0 planned_budget_amount,
  sum (inv_total) incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'INCOMPLETE_INVOICE' as source_key
FROM
  -- cg-gbq-p.enterprise_zone.cg_invoice_final_fact
  {{ ref('cg_invoice_final_fact') }}
WHERE
  complete_flag='N'
GROUP BY
  DATE(inv_date),
  COALESCE(inventory_item_id,0),
  cust_account_id,
  inv_number
UNION ALL
SELECT
  DATE(REQUEST_SHIP_DATE) date_key,
  inventory_item_id,
  cc.CUSTOMER_ACCOUNT_ID,
  order_number source_number,
  0 order_amount,
  0 invoice_amount,
  SUM (extended_amount) shipped_amount,
  0 awaiting_billing_amount,
  0 planned_budget_amount,
  0 incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'OPEN_ORDERS_SHIPPED' as source_key
FROM
  --cg-gbq-p.enterprise_zone.cg_sales_orders_inc_fact co
  {{ ref('cg_sales_orders_inc_fact') }} co
JOIN
  --cg-gbq-p.consumption_zone.cg_customer_class cc
  {{ ref('cg_dim_customer_class') }} cc
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
  order_number source_number,
  0 order_amount,
  0 invoice_amount,
  0 shipped_amount,
  SUM (extended_amount) awaiting_billing_amount,
  0 planned_budget_amount,
  0 incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'OPEN_ORDERS_AWAIT_BILLING' as source_key
FROM
  -- cg-gbq-p.enterprise_zone.cg_sales_orders_inc_fact co
  {{ ref('cg_sales_orders_inc_fact') }} co
JOIN
  -- cg-gbq-p.consumption_zone.cg_customer_class cc
  {{ ref('cg_dim_customer_class') }} cc
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
  date(due_date) date_key,
  0 inventory_item_id,
  cc.customer_account_id,
  '0' source_number,
  0 order_amount,
  0 invoice_amount,
  0 shipped_amount,
  0 awaiting_billing_amount,
  sum(amount) planned_budget_amount,
  0 incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'PLANNING DATA' as source_key
FROM
  -- cg-gbq-p.enterprise_zone.cg_planning_fact co
  {{ ref('cg_planning_fact') }} co
JOIN
  -- cg-gbq-p.consumption_zone.cg_customer_class cc
  {{ ref('cg_dim_customer_class') }} cc
ON
  co.account_name = cc.account_name
GROUP BY
  date(due_date),
  cc.customer_account_id

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
  SUM(Pretax_Total) AS invoice_amount,
  0 shipped_amount,
  0 AS awaiting_billing_amount,
  0 AS planned_budget_amount,
  0 AS incomplete_inv_amount,
  0 AS standard_cost,
  sum(quantity) invoiced_quantity,
  'WEBSITES DATA' AS source_key
FROM
  -- `cg-gbq-p.enterprise_zone.cg_websites_sales_fact`
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
  sum(amt) invoice_amount,
  0 shipped_amount,
  0 awaiting_billing_amount,
  0 planned_budget_amount,
  0 incomplete_inv_amount,
  0 standard_cost,
  0 invoiced_quantity,
  'LIGHT SPEED DATA' as source_key

 FROM
--  `cg-gbq-p.enterprise_zone.dg_sales_fact` 
{{ ref('dg_sales_fact') }}
group by 
date,
coalesce(inventory_item_id,0),
saleid
  )

SELECT 
md5(concat(date_key,inventory_item_id,customer_account_id,source_number,source_key)) as s_key,
date_key,
inventory_item_id,
customer_account_id,
source_number,
sum(order_amount) as  order_amount,
sum(invoice_amount) as invoice_amount,
sum(shipped_amount) as shipped_amount,
sum(awaiting_billing_amount) as awaiting_billing_amount,
sum(planned_budget_amount) as  planned_budget_amount,
sum(incomplete_inv_amount) as incomplete_inv_amount,
sum (standard_cost) as standard_cost,
sum(invoiced_quantity) as invoiced_quantity,
source_key as source_key
FROM  union_all_cte
group by 
date_key,
inventory_item_id,
customer_account_id,
source_number,
source_key