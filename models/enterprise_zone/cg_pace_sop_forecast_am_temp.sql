{{
       config(
             materialized='table',
             tags = 'cg_pace_sop_forecast_am_temp',
         )
}}


WITH
  forcast_temp AS (
  SELECT
    a.main_class_Category,
    a.main_class_sub_Category,
    a.account_name,
    a.main_Date,
    a.sop_amount AS previous_month_sop_forcast,
    COALESCE(b.customer_account_id,c.customer_account_id)customer_account_id
  FROM
    `cg-gbq-p.staging_zone.sop_forcast_source_data` a
  LEFT JOIN
    `consumption_zone.cg_customer_class` b
  ON
    a.main_class_Category = b.main_class_category
    AND UPPER(a.account_name) = UPPER(b.account_name)
    AND a.main_class_Category <> 'Wholesale Distribution'
  LEFT JOIN
    `consumption_zone.cg_customer_class` c
  ON
    a.main_class_Category = c.main_class_category
    AND a.main_class_sub_Category = c.main_class_sub_category
    AND UPPER(a.account_name) = UPPER(c.account_name)
    AND a.main_class_Category = 'Wholesale Distribution')
SELECT
  *
FROM
  forcast_temp