 {{
    config(
        materialized = 'view',
		tags = 'customer_sales_report_analysis_dummy'	
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
    '2022' as year_of_status,
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
    0 AS total_sales_2023,
    CASE
      WHEN a.status = 'LOST_AFTER_2021' THEN SUM(no_of_trans_2021)
      WHEN a.status = 'NEW_IN_2022' THEN SUM(no_of_trans_2022)
      WHEN a.status = 'REPEATS_FROM_2021' THEN SUM(no_of_trans_2022)
  END
    AS total_transaction,
    0 AS total_transaction_2023,
    CASE
      WHEN a.status = 'LOST_AFTER_2021' THEN SUM(no_of_items_2021)
      WHEN a.status = 'NEW_IN_2022' THEN SUM(no_of_items_2022)
      WHEN a.status = 'REPEATS_FROM_2021' THEN SUM(no_of_items_2022)
  END
    AS total_items,
    0 AS total_items_2023,
  FROM
    cte a
  LEFT JOIN
    distinct_count_status dcs
  ON
    a.status = dcs.status
  GROUP BY
    a.status,
    phone_number,
    store
  UNION ALL
  SELECT
    store,
    b.repeat_cx AS status,
    phone_number,
   '2022' as year_of_status,
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
    0 AS total_sales_2023,
    CASE
      WHEN b.repeat_cx = 'SPENDS_MORE_2022' THEN SUM(no_of_trans_2022)
      WHEN b.repeat_cx = 'SPENDS_LESS_2022' THEN SUM(no_of_trans_2022)
  END
    AS total_transaction,
    0 AS total_transaction_2023,
    CASE
      WHEN b.repeat_cx = 'SPENDS_MORE_2022' THEN SUM(no_of_items_2022)
      WHEN b.repeat_cx = 'SPENDS_LESS_2022' THEN SUM(no_of_items_2022)
  END
    AS total_items,
    0 AS total_items_2023
  FROM
    cte b
  LEFT JOIN
    distinct_count_repeat_cx dcr
  ON
    b.repeat_cx = dcr.repeat_cx
  GROUP BY
    b.repeat_cx,
    phone_number,
    store),
  cte_2023 AS (
  SELECT
    CASE
      WHEN sales_in_2022 >0 AND sales_in_2023 = 0 THEN 'LOST_AFTER_2022'
      WHEN sales_in_2023 > 0
    AND sales_in_2022 = 0 THEN "NEW_IN_2023"
      WHEN sales_in_2023 > 0 AND sales_in_2022 > 0 THEN "REPEATS_FROM_2022"
    ELSE
    "2024_REPORT"
  END
    AS status_2023,
    CASE
      WHEN sales_in_2023 > sales_in_2022 AND sales_in_2022 <> 0 AND sales_in_2023 <> 0 THEN "SPENDS_MORE_2023"
      WHEN sales_in_2023 < sales_in_2022
    AND sales_in_2022 <> 0
    AND sales_in_2023 <> 0 THEN "SPENDS_LESS_2023"
    ELSE
    "2024_REPORT"
  END
    AS repeat_cx_2023,
    *
  FROM
    cte),
  distinct_count_status_2023 AS (
  SELECT
    COUNT(DISTINCT phone_number) AS count_cx_2023,
    status_2023
  FROM
    cte_2023
  GROUP BY
    status_2023 ),
  distinct_count_repeat_cx_2023 AS (
  SELECT
    COUNT(DISTINCT phone_number) AS count_cx_2023,
    repeat_cx_2023
  FROM
    cte_2023
  GROUP BY
    repeat_cx_2023 ),
  final_cte_2023 AS (
  SELECT
    store,
    a.status_2023 AS status,
    phone_number,
    '2023' as year_of_status,
    SUM(sales_in_2021)sales_in_2021,
    SUM(no_of_trans_2021)no_of_trans_2021,
    SUM(no_of_items_2021)no_of_items_2021,
    SUM(sales_in_2022)sales_in_2022,
    SUM(no_of_trans_2022)no_of_trans_2022,
    SUM(no_of_items_2022)no_of_items_2022,
    SUM(sales_in_2023)sales_in_2023,
    SUM(no_of_trans_2023)no_of_trans_2023,
    SUM(no_of_items_2023)no_of_items_2023,
    SUM(count_cx_2023) AS unique_customers,
    COUNT(*) unique_cx,
    0 AS total_sales,
    CASE
      WHEN a.status_2023 = 'LOST_AFTER_2022' THEN SUM(sales_in_2022)
      WHEN a.status_2023 = 'NEW_IN_2023' THEN SUM(sales_in_2023)
      WHEN a.status_2023 = 'REPEATS_FROM_2022' THEN SUM(sales_in_2023)
  END
    AS total_sales_2023,
    0 AS total_transaction,
    CASE
      WHEN a.status_2023 = 'LOST_AFTER_2022' THEN SUM(no_of_trans_2022)
      WHEN a.status_2023 = 'NEW_IN_2023' THEN SUM(no_of_trans_2023)
      WHEN a.status_2023 = 'REPEATS_FROM_2022' THEN SUM(no_of_trans_2023)
  END
    AS total_transaction_2023,
    0 AS total_items,
    CASE
      WHEN a.status_2023 = 'LOST_AFTER_2022' THEN SUM(no_of_items_2022)
      WHEN a.status_2023 = 'NEW_IN_2023' THEN SUM(no_of_items_2023)
      WHEN a.status_2023 = 'REPEATS_FROM_2022' THEN SUM(no_of_items_2023)
  END
    AS total_items_2023,
  FROM
    cte_2023 a
  LEFT JOIN
    distinct_count_status_2023 dcs2023
  ON
    a.status_2023 = dcs2023.status_2023
  GROUP BY
    phone_number,
    store,
    a.status_2023
  UNION ALL
  SELECT
    store,
    b.repeat_cx_2023 AS status,
    phone_number,
    '2023' as year_of_status,
    SUM(sales_in_2021)sales_in_2021,
    SUM(no_of_trans_2021)no_of_trans_2021,
    SUM(no_of_items_2021)no_of_items_2021,
    SUM(sales_in_2022)sales_in_2022,
    SUM(no_of_trans_2022)no_of_trans_2022,
    SUM(no_of_items_2022)no_of_items_2022,
    SUM(sales_in_2023)sales_in_2023,
    SUM(no_of_trans_2023)no_of_trans_2023,
    SUM(no_of_items_2023)no_of_items_2023,
    SUM(count_cx_2023) AS unique_customers,
    COUNT(*) unique_cx,
    0 AS total_sales,
    CASE
      WHEN b.repeat_cx_2023 = 'SPENDS_MORE_2023' THEN SUM(sales_in_2023)
      WHEN b.repeat_cx_2023 = 'SPENDS_LESS_2023' THEN SUM(SALES_IN_2023)
  END
    AS total_sales_2023,
    0 AS total_transaction,
    COALESCE(
      CASE
        WHEN b.repeat_cx_2023 = 'SPENDS_MORE_2023' THEN SUM(no_of_trans_2023)
        WHEN b.repeat_cx_2023 = 'SPENDS_LESS_2023' THEN SUM(no_of_trans_2023)
    END
      ,0) AS total_transaction_2023,
    0 AS total_items,
    CASE
      WHEN b.repeat_cx_2023 = 'SPENDS_MORE_2023' THEN SUM(no_of_items_2023)
      WHEN b.repeat_cx_2023 = 'SPENDS_LESS_2023' THEN SUM(no_of_items_2023)
  END
    AS total_items_2023
  FROM
    cte_2023 b
  LEFT JOIN
    distinct_count_repeat_cx_2023 dcr_2023
  ON
    b.repeat_cx_2023 = dcr_2023.repeat_cx_2023
  GROUP BY
    phone_number,
    store,
    b.repeat_cx_2023),
  union_all_level_2 AS (
  SELECT
    *
  FROM
    final_cte
  UNION ALL
  SELECT
    *
  FROM
    final_cte_2023 )
SELECT
  DISTINCT *,
  DENSE_RANK() OVER (PARTITION BY status, phone_number ORDER BY status, phone_number) AS unique_customer_by_store
FROM
  union_all_level_2
WHERE
  status NOT IN('2023_REPORT',
    '2024_REPORT')