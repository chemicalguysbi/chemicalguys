
{{
       config(
             materialized='table',
             tags = 'cg_sku_sales_performance_report',
         )
}}

WITH
  date_item_cte AS (
  SELECT
    DISTINCT date_key,
    SKU,
    DESCRIPTION,
    a.INVENTORY_ITEM_ID AS item_inven_id,
    MAX(std_cost) AS standard_cost,
    a.ATTRIBUTE8 AS status,
    a.ATTRIBUTE9 AS category
  FROM
    `cg-gbq-p.oracle_nets.item_master` a
  CROSS JOIN (
    SELECT
      SUBSTR(CAST(DATE(date_key) AS string),0,7) AS date_key
    FROM
      `cg-gbq-p.consumption_zone.cg_date_dimension`
    WHERE
      DATE(date_key)>= DATE(CONCAT(CAST(CAST(EXTRACT(YEAR
              FROM
                DATE (current_date)) AS int64)-2 AS string),SUBSTR(CAST(current_date AS string),5)))
      AND DATE(date_key) <= current_date)
  GROUP BY
    1,
    2,
    4,
    3,
    6,
    7 ),
  forcast_fact_joined_cte AS (
  SELECT
    date_key,
    inv_amount,
    inv_amount/coalesce (b.invoiced_sales_units,
      0) AS avg_invoice_price,
    coalesce (b.invoiced_sales_units,
      0)invoiced_sales_units,
    fg_forcast_Sales,
    COALESCE(qty,0) AS qty_forcast,
    --a.inventory_item_id,
    SKU,
    DESCRIPTION,
    item_inven_id,
    standard_cost,
    status,
    category
  FROM
    date_item_cte a
  LEFT JOIN (
    SELECT
      SUM(inv_total) AS inv_amount,
      a.inventory_item_id,
      SUBSTR(CAST(inv_date AS string),0,7) AS yyyy_mm,
      COUNT(a.inventory_item_id) AS invoiced_sales_units,
    FROM
     -- `cg-gbq-p.enterprise_zone.cg_invoice_final_fact` 
       {{ ref('cg_invoice_final_fact') }} a
    WHERE
      DATE(inv_date)>= DATE(CONCAT(CAST(CAST(EXTRACT(YEAR
              FROM
                DATE (current_date)) AS int64)-2 AS string),SUBSTR(CAST(current_date AS string),5)))
      AND DATE(inv_date) <= current_date
    GROUP BY
      SUBSTR(CAST(inv_date AS string),0,7),
      inventory_item_id)b
  ON
    a.date_key = b.yyyy_mm
    AND CAST(a.item_inven_id AS int64) = b.inventory_item_id
  LEFT JOIN (
    SELECT
      SUM(a.USING_REQ_QTY)AS fg_forcast_Sales,
      b.inventory_item_id,
      SUBSTR(CAST(DATE(USING_ASSEMBLY_DEMAND_DATE) AS string),0,7) yyyy_mm,
      COUNT(a.item_number)qty
    FROM
     -- `cg-gbq-p.oracle_nets.demand_forecast` 
      {{ ref('cg_demand_forcast') }} a
    LEFT JOIN
      `cg-gbq-p.consumption_zone.cg_product_dimension` b
    ON
      a.item_number = b.item_number
    WHERE
      DATE(USING_ASSEMBLY_DEMAND_DATE)>= DATE(CONCAT(CAST(CAST(EXTRACT(YEAR
              FROM
                DATE (current_date)) AS int64)-2 AS string),SUBSTR(CAST(current_date AS string),5)))
      AND DATE(USING_ASSEMBLY_DEMAND_DATE) <= current_date
    GROUP BY
      SUBSTR(CAST(DATE(USING_ASSEMBLY_DEMAND_DATE) AS string),0,7),
      inventory_item_id)c
  ON
    a.date_key = c.yyyy_mm
    AND CAST(a.item_inven_id AS int64) = c.inventory_item_id)
SELECT
  sku,
  description,
  item_inven_id,
  status,
  category,
  date_key,
  inv_amount,
  avg_invoice_price,
  invoiced_sales_units,
  fg_forcast_Sales,
  qty_forcast,
  standard_cost
FROM
  forcast_fact_joined_cte