{{
    config(
        materialized = 'view',
		tags = 'customer_year_wise_report'	
    )
}}

WITH
  year_wise_cx_report AS (
  SELECT store,
    CAST(EXTRACT(YEAR
      FROM
        DATE (Completed_Date)) AS STRING) AS year,
    Phone_Number,
    SUM(total) total_sales,
    COUNT(completed_date) AS total_transections,
    SUM(__of_Items) no_of_items
  FROM
    `cg-gbq-p.lightspeed.z_dg_customer_sales`
  GROUP BY
    1,
    2,3),
overall_year_report as (
SELECT store,
  year,
  phone_number,
  COUNT(Phone_Number) total_unique_customers,
  SUM(total_sales)total_sales,
  CASE
    WHEN COUNT(phone_number) = 0 THEN 0
  ELSE
  SUM(total_sales)/COUNT(Phone_Number) end AS customer_per,
  SUM(total_transections) AS total_transactions,
  CASE
    WHEN SUM(total_transections) = 0 THEN 0
    ELSE
    SUM(total_sales)/SUM(total_transections) end AS transaction_per,SUM(no_of_items) AS total_items,CASE
      WHEN SUM(no_of_items) = 0 THEN 0
    ELSE
    SUM(total_sales)/SUM(no_of_items)end  AS items_per
  FROM
    year_wise_cx_report
  GROUP BY
    1,2,3
  ORDER BY
    year desc)

    ,output_check as (
select a.*,coalesce(b.repeat_customer,0) repeat_customer from overall_year_report a
left join
(SELECT substring(status,14)status,count(unique_customer_by_store)repeat_customer FROM `cg-gbq-p.consumption_zone.customer_sales_report_analysis` 
where status like 'REPEAT%'
group by 1) b
on 
a.year = cast(cast(b.status as int64)+1 as string))
select *
 from output_check