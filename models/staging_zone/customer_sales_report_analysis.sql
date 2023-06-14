{{
    config(
        materialized = 'view',
		tags = 'customer_sales_report_analysis'	
    )
}}

WITH
  cte AS (
  SELECT
    CASE
      WHEN sales_in_2021 >0 AND sales_in_2022 = 0 THEN 'LOST_AFTER_2021'
      WHEN sales_in_2022 > 0
    AND sales_in_2021 = 0 THEN "NEW_IN_2022"
      WHEN sales_in_2022 > 0 AND sales_in_2021 > 0 THEN "REPEATS_FROM_2021"
    ELSE
    "2023_REPORT"
  END
    AS status,
    CASE
      WHEN sales_in_2022 > sales_in_2021 AND sales_in_2021 <> 0 AND sales_in_2022 <> 0 THEN "SPENDS_MORE_2022"
      WHEN sales_in_2022 < sales_in_2021
    AND sales_in_2021 <> 0
    AND sales_in_2022 <> 0 THEN "SPENDS_LESS_2022"
    ELSE
    "2023_REPORT"
  END
    AS repeat_cx,
    *
  FROM
    `cg-gbq-p.lightspeed.z_dg_customer_sales_summarized`),
  distinct_count_status AS (
  SELECT
    COUNT(DISTINCT phone_number) AS count_cx,
    status
  FROM
    cte
  GROUP BY
    status ),
  distinct_count_repeat_cx AS (
  SELECT
    COUNT(DISTINCT phone_number) AS count_cx,
    repeat_cx
  FROM
    cte
  GROUP BY
    repeat_cx ),
  final_cte AS (
  SELECT
    store,
    a.status,
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
    SUM(count_cx)unique_customers,
    COUNT(*) unique_cx,
    CASE
      WHEN a.status = 'LOST_AFTER_2021' THEN SUM(sales_in_2021)
      WHEN a.status = 'NEW_IN_2022' THEN SUM(sales_in_2022)
      WHEN a.status = 'REPEATS_FROM_2021' THEN SUM(sales_in_2022)
  END
    AS total_sales,
    CASE
      WHEN a.status = 'LOST_AFTER_2021' THEN SUM(no_of_trans_2021)
      WHEN a.status = 'NEW_IN_2022' THEN SUM(no_of_trans_2022)
      WHEN a.status = 'REPEATS_FROM_2021' THEN SUM(no_of_trans_2022)
  END
    AS total_transaction,
    CASE
      WHEN a.status = 'LOST_AFTER_2021' THEN SUM(no_of_items_2021)
      WHEN a.status = 'NEW_IN_2022' THEN SUM(no_of_items_2022)
      WHEN a.status = 'REPEATS_FROM_2021' THEN SUM(no_of_items_2022)
  END
    AS total_items,
  FROM
    cte a
  LEFT JOIN
    distinct_count_status dcs
  ON
    a.status = dcs.status
  GROUP BY
    status,
    phone_number,
    store
  UNION ALL
  SELECT
    store,
    b.repeat_cx AS status,
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
    SUM(count_cx)unique_customers,
    COUNT(*) unique_cx,
    CASE
      WHEN b.repeat_cx = 'SPENDS_MORE_2022' THEN SUM(sales_in_2022)
      WHEN b.repeat_cx = 'SPENDS_LESS_2022' THEN SUM(SALES_IN_2022)
  END
    AS total_sales,
    CASE
      WHEN b.repeat_cx = 'SPENDS_MORE_2022' THEN SUM(no_of_trans_2022)
      WHEN b.repeat_cx = 'SPENDS_LESS_2022' THEN SUM(no_of_trans_2022)
  END
    AS total_transaction,
    CASE
      WHEN b.repeat_cx = 'SPENDS_MORE_2022' THEN SUM(no_of_items_2022)
      WHEN b.repeat_cx = 'SPENDS_LESS_2022' THEN SUM(no_of_items_2022)
  END
    AS total_items
  FROM
    cte b
  LEFT JOIN
    distinct_count_repeat_cx dcr
  ON
    b.repeat_cx = dcr.repeat_cx
  GROUP BY
    b.repeat_cx,
    phone_number,
    store)
SELECT
 *
FROM
  final_cte
WHERE
  status <> '2023_REPORT'

