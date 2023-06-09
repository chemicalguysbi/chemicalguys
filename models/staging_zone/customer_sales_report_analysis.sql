{{
    config(
        materialized = 'view',
		tags = 'customer_sales_report_analysis'	
    )
}}

with cte as (SELECT
  CASE
    WHEN sales_in_2021 >0 AND sales_in_2022 = 0 THEN 'LOST_AFTER_2021'
  
    WHEN sales_in_2022 > 0 AND sales_in_2021 = 0 THEN "NEW_IN_2022"
  
    WHEN sales_in_2022 > 0
  AND sales_in_2021 > 0 then "REPEATS_FROM_2021" else "2023_REPORT" end as status,
  case when sales_in_2022 > sales_in_2021  then "SPENDS_MORE_2022"
  WHEN sales_in_2022 < sales_in_2021 THEN "SPENDS_LESS_2022"  ELSE "2023_REPORT" End as repeat_cx,*
FROM
  `cg-gbq-p.lightspeed.z_dg_customer_sales_summarized`)

--select distinct status from cte where repeat_cx in ('SPENDS_MORE','SPENDS_LESS')

-- ,final_cte as (
--   select status,count(*) as customer_unique, sum(sales_in_2021)sales_in_2021,sum(sales_in_2022)sales_in_2022 from cte
--   group by status
-- union all
-- select 
-- repeat_cx as status,count(*) as customer_unique, sum(sales_in_2021)sales_in_2021,sum(sales_in_2022)sales_in_2022 
-- from cte
--   group by repeat_cx)
--select * from final_cte where status <> '2023_REPORT'


,final_cte AS (
  SELECT store,
    status,phone_number,sum(sales_in_2021)sales_in_2021, sum(no_of_trans_2021)no_of_trans_2021, sum(no_of_items_2021)no_of_items_2021, sum(sales_in_2022)sales_in_2022, sum(no_of_trans_2022)no_of_trans_2022, sum(no_of_items_2022)no_of_items_2022, sum(sales_in_2023)sales_in_2023, sum(no_of_trans_2023)no_of_trans_2023, sum(no_of_items_2023)no_of_items_2023,
    COUNT(*) unique_cx,
    CASE
      WHEN status = 'LOST_AFTER_2021' THEN SUM(sales_in_2021)
      WHEN status = 'NEW_IN_2022' THEN SUM(sales_in_2022)
      WHEN status = 'REPEATS_FROM_2021' THEN SUM(sales_in_2022) --
      -- WHEN repeat_cx = 'SPENDS_MORE_2022' THEN SUM(sales_in_2022) --
      -- WHEN repeat_cx = 'SPENDS_LESS_2022' THEN SUM(SALES_IN_2022)
  END
    AS TOTAL_SALES,
    CASE
      WHEN status = 'LOST_AFTER_2021' THEN SUM(no_of_trans_2021)
      WHEN status = 'NEW_IN_2022' THEN SUM(no_of_trans_2022)
      WHEN status = 'REPEATS_FROM_2021' THEN SUM(no_of_trans_2022)
  END
    AS TOTAL_transaction,
    CASE
      WHEN status = 'LOST_AFTER_2021' THEN SUM(no_of_items_2021)
      WHEN status = 'NEW_IN_2022' THEN SUM(no_of_items_2022)
      WHEN status = 'REPEATS_FROM_2021' THEN SUM(no_of_items_2022)
  END
    AS TOTAL_items,
  FROM
    cte
  GROUP BY
    status,phone_number,store
  UNION ALL
  SELECT store,
    repeat_cx AS status,phone_number,sum(sales_in_2021)sales_in_2021, sum(no_of_trans_2021)no_of_trans_2021, sum(no_of_items_2021)no_of_items_2021, sum(sales_in_2022)sales_in_2022, sum(no_of_trans_2022)no_of_trans_2022, sum(no_of_items_2022)no_of_items_2022, sum(sales_in_2023)sales_in_2023, sum(no_of_trans_2023)no_of_trans_2023, sum(no_of_items_2023)no_of_items_2023,
    COUNT(*) unique_cx,
    CASE
      WHEN repeat_cx = 'SPENDS_MORE_2022' THEN SUM(sales_in_2022)
      WHEN repeat_cx = 'SPENDS_LESS_2022' THEN SUM(SALES_IN_2022)
  END
    AS TOTAL_SALES,
    CASE
      WHEN repeat_cx = 'SPENDS_MORE_2022' THEN SUM(no_of_trans_2022)
      WHEN repeat_cx = 'SPENDS_LESS_2022' THEN SUM(no_of_trans_2022)
  END
    AS TOTAL_transaction,
    CASE
      WHEN repeat_cx = 'SPENDS_MORE_2022' THEN SUM(no_of_items_2022)
      WHEN repeat_cx = 'SPENDS_LESS_2022' THEN SUM(no_of_items_2022)
  END
    AS TOTAL_items
  FROM
    cte
  GROUP BY
    repeat_cx,phone_number,store)
SELECT
  *,
  case when unique_cx = 0 then 0 else total_sales/unique_cx end AS cutomer_per,
  case when total_transaction = 0 then 0 else total_sales/total_transaction end AS transaction_per,
  case when total_items = 0 then 0 else total_sales/total_items end  AS item_per,case when sales_in_2021 >0  and   sales_in_2021 > 0 and sales_in_2022 > sales_in_2021 then sales_in_2021 end as sales_in_2021_2022
FROM
  final_cte
WHERE
  status <> '2023_REPORT'


