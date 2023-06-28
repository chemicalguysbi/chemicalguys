{{
       config(
             materialized='view',
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
    a.ATTRIBUTE9 AS category,
    ORGANIZATION_CODE
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
<<<<<<< HEAD
      WHERE upper(CLASS) = 'START QOH'
=======
      WHERE UPPER(CLASS) = 'START QOH'
>>>>>>> e8577480ba7c1b803f1f3bb4be6fda656167472c
  GROUP BY
    1,
    2,
    4,
    3,
    6,
    7,
    8 ),
  forcast_fact_joined_cte AS (
  SELECT
    a.date_key,
    COALESCE(inv_amount,0)inv_amount,
    COALESCE(inv_amount/coalesce (b.invoiced_sales_units,
        0),0) AS avg_invoice_price,
    coalesce (b.invoiced_sales_units,
      0)invoiced_sales_units,
    COALESCE(fg_forcast_Sales,0)fg_forcast_Sales,
    COALESCE(qty,0) AS qty_forcast,
    --a.inventory_item_id,
    SKU,
    DESCRIPTION,
    item_inven_id,
    a.ORGANIZATION_CODE,
    COALESCE(standard_cost,0)standard_cost,
    status,
    category,
    COALESCE(QOH,0) AS oracle_inventory
  FROM
    date_item_cte a
  LEFT JOIN (
    SELECT
      SUM(inv_total) AS inv_amount,
      a.inventory_item_id,
      SUBSTR(CAST(inv_date AS string),0,7) AS yyyy_mm,
      COUNT(a.inventory_item_id) AS invoiced_sales_units,
    FROM
     -- `cg-gbq-p.enterprise_zone.cg_invoice_final_fact` a
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
      --`cg-gbq-p.oracle_nets.demand_forecast` a 
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
    AND CAST(a.item_inven_id AS int64) = c.inventory_item_id
  LEFT JOIN (
    SELECT
      ORGANIZATION_CODE,
      INVENTORY_ITEM_ID,
      ITEM_NUMBER,
      MAX(QOH)QOH,
      SUBSTR(CAST(DATE(date_key) AS string),0,7) AS yyyy_mm
    FROM
      --`cg-gbq-p.oracle_nets.inventory_on_hand` a
      {{ ref('cg_inventory_on_hand') }} a
    LEFT JOIN
      `cg-gbq-p.consumption_zone.cg_date_dimension` b
    ON
      CAST(a.WEEK AS string) = b.week_in_year
      AND A.YEAR = b.year_number
      AND DATE(date_key)>= DATE(CONCAT(CAST(CAST(EXTRACT(YEAR
              FROM
                DATE (current_date)) AS int64)-2 AS string),SUBSTR(CAST(current_date AS string),5)))
      AND DATE(date_key) <= current_date
    GROUP BY
      1,
      2,
      3,
      5)d
  ON
    a.date_key = d.yyyy_mm
    AND CAST(a.item_inven_id AS int64) = d.inventory_item_id
    AND a.ORGANIZATION_CODE = d.ORGANIZATION_CODE )

    -- select * from forcast_fact_joined_cte
    -- where item_inven_id = 100000000458119


SELECT
  sku,
  description,
  item_inven_id,
  status,
  category,
  ORGANIZATION_CODE as organization_code,
  SUM(oracle_inventory)oracle_inventory,
  date_key,
  SUM(inv_amount)inv_amount,
  SUM(avg_invoice_price)avg_invoice_price,
  SUM(invoiced_sales_units)invoiced_sales_units,
  SUM(fg_forcast_Sales)fg_forcast_Sales,
  SUM(qty_forcast)qty_forcast,
  SUM(standard_cost)standard_cost
FROM
  forcast_fact_joined_cte 
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,8
