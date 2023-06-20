{{
       config(
              materialized='view',
               tags = 'lightspeed_customer_report',
             )
}}


with lightspeed_report  as (select phone_number as account_account_id, sum(total) sales_2021,0 as sales_2022,0 as sales_2023, count(Completed_Date) as number_of_transections_2021,0 as number_of_transections_2022,0 as number_of_transections_2023 
from 
--`cg-gbq-p.lightspeed.customer_sales_fact`
{{ ref('customer_sales_fact') }}
where cast(EXTRACT(YEAR FROM DATE (Completed_Date)) AS STRING) = '2021'
group by 1
union all
select phone_number as account_account_id, 0 as sales_2021,sum(total) sales_2022,0 as sales_2023, 0 as number_of_transections_2021,count(Completed_Date) as number_of_transections_2022,0 as number_of_transections_2023 
from 
--`cg-gbq-p.lightspeed.customer_sales_fact`
{{ ref('customer_sales_fact') }}

where cast(EXTRACT(YEAR FROM DATE (Completed_Date)) AS STRING) = '2022'
group by 1
union all
select phone_number as account_account_id, 0 as sales_2021,0 as sales_2022, sum(total) sales_2023,0 as number_of_transections_2021,
0 as number_of_transections_2022,
count(Completed_Date) as number_of_transections_2023 
from 
--`cg-gbq-p.lightspeed.customer_sales_fact`
{{ ref('customer_sales_fact') }}
where cast(EXTRACT(YEAR FROM DATE (Completed_Date)) AS STRING) = '2023'
group by 1
)
select account_account_id,sum(sales_2021) as sales_in_2021,sum(number_of_transections_2021)
as no_of_trans_2021,sum(sales_2022) as sales_in_2022,sum(number_of_transections_2022) as no_of_trans_2022,sum(sales_2023) as sales_in_2023,sum(number_of_transections_2023) as no_of_trans_2023
 from lightspeed_report group by 1