
{{
       config(
             materialized='view',
             tags = 'cg_customer_percentile_data_report',
         )
}}


WITH
  core_cte AS (
  SELECT
    *,
    CASE
      WHEN percentile_2021 >=0.9 THEN "percent_90"
      WHEN (percentile_2021 >=0.8
      AND percentile_2021 < 0.9) THEN "percent_80"
      WHEN (percentile_2021 >= 0.7 AND percentile_2021 <0.8) THEN "percent_70"
      WHEN (percentile_2021 >=0.6
      AND percentile_2021 <0.7) THEN "percent_60"
      WHEN (percentile_2021 >= 0.5 AND percentile_2021 <0.6) THEN "percent_50"
      WHEN (percentile_2021 >= 0.4
      AND percentile_2021 <0.5) THEN "percent_40"
      WHEN (percentile_2021 >= 0.3 AND percentile_2021 <0.4) THEN "percent_30"
      WHEN (percentile_2021 >= 0.2
      AND percentile_2021 <0.3) THEN "percent_20"
      WHEN (percentile_2021 >= 0.1 AND percentile_2021 <0.2) THEN "percent_10"
      WHEN (percentile_2021 >= 0.0
      AND percentile_2021 <0.1) THEN "percent_0"
  END
    AS percentage_2021,
 
    CASE
      WHEN percentile_2022 >=0.9 THEN "percent_90"
      WHEN (percentile_2022 >=0.8
      AND percentile_2022 < 0.9) THEN "percent_80"
      WHEN (percentile_2022 >= 0.7 AND percentile_2022 <0.8) THEN "percent_70"
      WHEN (percentile_2022 >=0.6
      AND percentile_2022 <0.7) THEN "percent_60"
      WHEN (percentile_2022 >= 0.5 AND percentile_2022 <0.6) THEN "percent_50"
      WHEN (percentile_2022 >= 0.4
      AND percentile_2022 <0.5) THEN "percent_40"
      WHEN (percentile_2022 >= 0.3 AND percentile_2022 <0.4) THEN "percent_30"
      WHEN (percentile_2022 >= 0.2
      AND percentile_2022 <0.3) THEN "percent_20"
      WHEN (percentile_2022 >= 0.1 AND percentile_2022 <0.2) THEN "percent_10"
      WHEN (percentile_2022 >= 0.0
      AND percentile_2022 <0.1) THEN "percent_0"
  END
    AS percentage_2022,
    CASE
      WHEN percentile_2023 >=0.9 THEN "percent_90"
      WHEN (percentile_2023 >=0.8
      AND percentile_2023 < 0.9) THEN "percent_80"
      WHEN (percentile_2023 >= 0.7 AND percentile_2023 <0.8) THEN "percent_70"
      WHEN (percentile_2023 >=0.6
      AND percentile_2023 <0.7) THEN "percent_60"
      WHEN (percentile_2023 >= 0.5 AND percentile_2023 <0.6) THEN "percent_50"
      WHEN (percentile_2023 >= 0.4
      AND percentile_2023 <0.5) THEN "percent_40"
      WHEN (percentile_2023 >= 0.3 AND percentile_2023 <0.4) THEN "percent_30"
      WHEN (percentile_2023 >= 0.2
      AND percentile_2023 <0.3) THEN "percent_20"
      WHEN (percentile_2023 >= 0.1 AND percentile_2023 <0.2) THEN "percent_10"
      WHEN (percentile_2023 >= 0.0
      AND percentile_2023 <0.1) THEN "percent_0"
  END
    AS percentage_2023,
    CASE
      WHEN percentile_2021 >=0.9 AND percentile_2022 >=0.9 THEN "percent_90"
      WHEN (percentile_2021 >=0.8
      AND percentile_2021 < 0.9
      AND percentile_2022 >=0.8
      AND percentile_2022 < 0.9 ) THEN "percent_80"
      WHEN (percentile_2021 >= 0.7 AND percentile_2021 <0.8 AND percentile_2022 >= 0.7 AND percentile_2022 <0.8) THEN "percent_70"
      WHEN (percentile_2021 >=0.6
      AND percentile_2021 <0.7
      AND percentile_2022 >=0.6
      AND percentile_2022 <0.7) THEN "percent_60"
      WHEN (percentile_2021 >= 0.5 AND percentile_2021 <0.6 AND percentile_2022 >= 0.5 AND percentile_2022 <0.6) THEN "percent_50"
      WHEN (percentile_2021 >= 0.4
      AND percentile_2021 <0.5
      AND percentile_2022 >= 0.4
      AND percentile_2022 <0.5) THEN "percent_40"
      WHEN (percentile_2021 >= 0.3 AND percentile_2021 <0.4 AND percentile_2022 >= 0.3 AND percentile_2022 <0.4) THEN "percent_30"
      WHEN (percentile_2021 >= 0.2
      AND percentile_2021 <0.3
      AND percentile_2022 >= 0.2
      AND percentile_2022 <0.3) THEN "percent_20"
      WHEN (percentile_2021 >= 0.1 AND percentile_2021 <0.2 AND percentile_2022 >= 0.1 AND percentile_2022 <0.2) THEN "percent_10"
      WHEN (percentile_2021 >= 0.0
      AND percentile_2021 <0.1
      AND percentile_2022 >= 0.0
      AND percentile_2022 <0.1) THEN "percent_0"
    ELSE
    "non_repeat_cx"
  END
    AS repeat_cx_status
  FROM
    `cg-gbq-p.lightspeed.z_dg_customer_sales_summarized`
    --the output of the above code retrieves all the columns from the source table and also adds extra 4 columns which denotes as to which cataegory the percentile data falls
    ),
  percentage_union_all_cte AS (
  SELECT
    percentage_2021 AS percentage,
    MIN(sales_in_2021)sales_in_2021,
    0 AS sales_in_2022,
    0 AS sales_in_2023,
    0 AS repeat_cx
  FROM
    core_cte
  GROUP BY
    1
  UNION ALL
  SELECT
    percentage_2022 AS percentage,
    0 AS sales_in_2021,
    MIN(sales_in_2022)sales_in_2022,
    0 AS sales_in_2023,
    0 AS repeat_cx
  FROM
    core_cte
  GROUP BY
    1
  UNION ALL
  SELECT
    percentage_2023 AS percentage,
    0 AS sales_in_2021,
    0 AS sales_in_2022,
    MIN(sales_in_2023)sales_in_2023,
    0 AS repeat_cx
  FROM
    core_cte
  GROUP BY
    1
  UNION ALL
  SELECT
    repeat_cx_status AS percentage,
    0 AS sales_in_2021,
    0 AS sales_in_2022,
    0 AS sales_in_2023,
    COUNT(DISTINCT phone_number) AS repeat_cx
  FROM
    core_cte
  GROUP BY
    1
    -- percentage cte used to get the min values (threshold values) of the aggregate columns and also the count of distinct repeated customers
    ),
  percentage_cte AS (
  SELECT
    percentage,
    SUM(sales_in_2021)threshold_in_2021,
    SUM(sales_in_2022)threshold_in_2022,
    SUM(sales_in_2023)threshold_in_2023,
    SUM(DISTINCT repeat_cx) AS repeat_customers
  FROM
    percentage_union_all_cte
  GROUP BY
    1),
  detail_cte AS (
  SELECT
    d.percentage,
    d.threshold_in_2021,
    0 AS threshold_in_2022,
    0 AS threshold_in_2023,
    per_2021.store,
    per_2021.phone_number,
    per_2021.sales_in_2021,
    per_2021.no_of_trans_2021,
    per_2021.no_of_items_2021,
    0 AS sales_in_2022,
    0 AS no_of_trans_2022,
    0 AS no_of_items_2022,
    0 AS sales_in_2023,
    0 AS no_of_trans_2023,
    0 AS no_of_items_2023,
    CASE
      WHEN per_2021.sales_in_2021 >= d.threshold_in_2021 THEN 1
    ELSE
    0
  END
    AS no_of_cx_2021,
    0 AS no_of_cx_2022,
    0 AS no_of_cx_2023,
    d.repeat_customers
  FROM
    percentage_cte d
  LEFT JOIN
    core_cte per_2021
  ON
    d.percentage = percentage_2021
  UNION ALL
  SELECT
    d.percentage,
    0 AS threshold_in_2021,
    d.threshold_in_2022,
    0 AS threshold_in_2023,
    per_2022.store,
    per_2022.phone_number,
    0 AS sales_in_2021,
    0 AS no_of_trans_2021,
    0 AS no_of_items_2021,
    per_2022.sales_in_2022,
    per_2022.no_of_trans_2022,
    per_2022.no_of_items_2022,
    0 AS sales_in_2023,
    0 AS no_of_trans_2023,
    0 AS no_of_items_2023,
    0 AS no_of_cx_2021,
    CASE
      WHEN per_2022.sales_in_2022 >= d.threshold_in_2022 THEN 1
    ELSE
    0
  END
    AS no_of_cx_2022,
    0 AS no_of_cx_2023,
    d.repeat_customers
  FROM
    percentage_cte d
  LEFT JOIN
    core_cte per_2022
  ON
    d.percentage = percentage_2022
  UNION ALL
  SELECT
    d.percentage,
    0 AS threshold_in_2021,
    0 AS threshold_in_2022,
    d.threshold_in_2023,
    per_2023.store,
    per_2023.phone_number,
    0 AS sales_in_2021,
    0 AS no_of_trans_2021,
    0 AS no_of_items_2021,
    0 AS sales_in_2022,
    0 AS no_of_trans_2022,
    0 AS no_of_items_2022,
    per_2023.sales_in_2023,
    per_2023.no_of_trans_2023,
    per_2023.no_of_items_2023,
    0 AS no_of_cx_2021,
    0 AS no_of_cx_2022,
    CASE
      WHEN per_2023.sales_in_2023 >= d.threshold_in_2023 THEN 1
    ELSE
    0
  END
    AS no_of_cx_2023,
    d.repeat_customers
  FROM
    percentage_cte d
  LEFT JOIN
    core_cte per_2023
  ON
    d.percentage = percentage_2023)
SELECT
  percentage,
  MAX(threshold_in_2021)threshold_in_2021,
  MAX(threshold_in_2022)threshold_in_2022,
  MAX(threshold_in_2023)threshold_in_2023,
  SUM(no_of_cx_2021)no_of_cx_2021,
  SUM(no_of_cx_2022)no_of_cx_2022,
  SUM(no_of_cx_2023)no_of_cx_2023,
  store,
  phone_number,
  SUM(sales_in_2021)sales_in_2021,
  SUM(no_of_trans_2021)no_of_trans_2021,
  SUM(no_of_items_2021)no_of_items_2021,
  SUM(sales_in_2022)sales_in_2022,
  SUM(no_of_trans_2022)no_of_trans_2022,
  SUM(no_of_items_2022)no_of_items_2022,
  SUM(sales_in_2023)sales_in_2023,
  SUM(no_of_trans_2023)no_of_trans_2023,
  SUM(no_of_items_2023)no_of_items_2023,
  MAX(b.total_sale_in_2021)total_sale_in_2021,
  MAX(total_sale_in_2022)total_sale_in_2022,
  MAX(total_sale_in_2023)AS total_sale_in_2023,
  MAX(total_cx_in_2021)total_cx_in_2021,
  MAX(total_cx_in_2022)total_cx_in_2022,
  MAX(total_cx_in_2023)total_cx_in_2023,
  MAX(repeat_customers)rep_cx_from_2021,
  MAX(total_repeat_customers)total_rep_cx_from_2021,
  max(case when sales_in_2021 <>0 then substring(percentage,9) else '0' end) as percentage_2021,
max(case when sales_in_2022 <>0 then substring(percentage,9) else '0' end) as percentage_2022,
max(case when sales_in_2023 <>0 then substring(percentage,9) else '0' end) as percentage_2023 ,
FROM
  detail_cte a
CROSS JOIN (
  SELECT
    SUM(sales_in_2021) total_sale_in_2021,
    SUM(sales_in_2022)total_sale_in_2022,
    SUM(sales_in_2023)AS total_sale_in_2023,
    SUM(no_of_cx_2021)total_cx_in_2021,
    SUM(no_of_cx_2022)total_cx_in_2022,
    SUM(no_of_cx_2023)total_cx_in_2023,
    SUM(DISTINCT repeat_customers)total_repeat_customers
  FROM
    detail_cte
  WHERE
    percentage <> 'non_repeat_cx')b
    where percentage <> 'non_repeat_cx'
GROUP BY
  store,
  phone_number,
  percentage
ORDER BY
  percentage DESC