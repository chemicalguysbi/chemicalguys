{{
       config(
              materialized='view',
               tags = 'salesforce_orderheader_sales_report',
             )
}}
with salesforce_report as (select account_account_id ,account_account_name ,sum(Pretax_Subtotal) sales_2021,0 as sales_2022,0 as sales_2023, count(Created_Date) as number_of_transections_2021,0 as number_of_transections_2022,0 as number_of_transections_2023 from 

`cg-gbq-p.staging_zone.sales_force_order_header_view` 

where cast(EXTRACT(YEAR FROM DATE (Created_Date)) AS STRING) = '2021'
group by 1,2
union all
select account_account_id,account_account_name, 0 as sales_2021,sum(Pretax_Subtotal) sales_2022,0 as sales_2023, 0 as number_of_transections_2021,count(Created_Date) as number_of_transections_2022,0 as number_of_transections_2023 from `cg-gbq-p.staging_zone.sales_force_order_header_view` 

where cast(EXTRACT(YEAR FROM DATE (Created_Date)) AS STRING) = '2022'
group by 1,2
union all
select account_account_id,account_account_name, 0 as sales_2021,0 as sales_2022, sum(Pretax_Subtotal) sales_2023,0 as number_of_transections_2021,
0 as number_of_transections_2022,
count(Created_Date) as number_of_transections_2023 from `cg-gbq-p.staging_zone.sales_force_order_header_view` 

where cast(EXTRACT(YEAR FROM DATE (Created_Date)) AS STRING) = '2023'
group by 1,2
)
select account_account_id,account_account_name, sum(sales_2021) as sales_in_2021,sum(sales_2022) as sales_in_2022,sum(sales_2023) as sales_in_2023,sum(number_of_transections_2021)
as no_of_trans_2021, sum(number_of_transections_2022) as no_of_trans_2022,sum(number_of_transections_2023) as no_of_trans_2023
 from salesforce_report group by 1,2