{{
       config(
             materialized='view',
             tags = 'cg_pace_sop_forecast_am'
         )
   }}


WITH
  planning_data_cte AS (
  SELECT
    DISTINCT b.main_class_category,
    b.main_class_sub_category,
    main_class,
    due_date,
    amount,
    customer_account_id
  FROM (
    SELECT
      a.*,
      b.account_name AS cc_account_name,
      b.main_class_category,
      b.main_class_sub_category,
      b.customer_account_id
    FROM
      --`cg-gbq-p.enterprise_zone.cg_planning_fact` a

      {{ ref('cg_planning_fact') }} a
    LEFT JOIN
      `consumption_zone.cg_customer_class` b
    ON
      UPPER(a.account_name) = UPPER(b.account_name) 
      ) b
  WHERE
    main_class_category IS NOT NULL ),
  detail_cte AS (
  SELECT
    fc.*,
    pd.amount AS planned_amount,
    pdc.amount AS planned_amount_wd,
    plan_amount,
    plan_amount_wd,
    pd.customer_account_id,
    pdc.customer_account_id AS cust_ac_id
  FROM
   -- `cg-gbq-p.staging_zone.cg_pace_sop_forecast` fc
    {{ ref('cg_pace_sop_forecast') }} fc
  LEFT JOIN
    planning_data_cte pd
  ON
    fc.main_class_category = pd.main_class_category
    AND pd.main_class_category <> 'Wholesale Distribution'
    AND main_date = due_date
  LEFT JOIN
    planning_data_cte pdc
  ON
    fc.main_class_category = pdc.main_class_category
    AND fc.main_class_sub_category = pdc.main_class_sub_category
    AND main_date = pdc.due_date
    AND pdc.main_class_category = 'Wholesale Distribution'
  LEFT JOIN (
    SELECT
      SUM(amount)plan_amount,
      main_class_category,
      due_date
    FROM
      planning_data_cte
    WHERE
      main_class_category <> 'Wholesale Distribution'
    GROUP BY
      2,
      3) a
  ON
    fc.main_class_category = a.main_class_category
    AND main_date = a.due_date
  LEFT JOIN (
    SELECT
      SUM(amount)plan_amount_wd,
      main_class_category,
      main_class_sub_category,
      due_date
    FROM
      planning_data_cte
    WHERE
      main_class_category = 'Wholesale Distribution'
    GROUP BY
      2,
      3,
      4) b
  ON
    fc.main_class_category = b.main_class_category
    AND fc.main_class_sub_category = b.main_class_sub_category
    AND main_date = b.due_date ),
  columunar_data_cte AS (
  SELECT
    main_class_Category,
    main_class_sub_Category,
    amount,
    main_Date,
    COALESCE(planned_amount,planned_amount_wd) AS planned_amount_coel,
    planned_amount,
    planned_amount_wd,
    COALESCE(plan_amount,plan_amount_wd) AS plan_amount,
    COALESCE(customer_account_id,cust_ac_id)customer_account_id,
  FROM
    detail_cte )
SELECT
  main_class_Category,
  main_class_sub_Category,
  customer_account_id,
  amount,
  main_Date,
  planned_amount_coel AS total_planning_amount,
  plan_amount,
  (planned_amount_coel/plan_amount) *100 AS percentage_calculation,
  (amount*((planned_amount_coel/plan_amount) *100))/100 AS sop_amount
FROM
  columunar_data_cte