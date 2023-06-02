{{
    config(
        materialized = 'table',
		tags = 'cg_planning_fact'
    )
}}
with customer_class as (
SELECT 
account_name,
upper(account_name) as account_name_upper,			
account_number,			
customer_account_id,				
customer_type,			
customer_class_code,							
customer_category,				
main_class,
cust_id,
main_class_category,
main_class_sub_category

FROM 
-- `cg-gbq-p.consumption_zone.cg_customer_class`
{{ ref('cg_customer_class') }}
)

,planning_data as (
SELECT account_name,upper(account_name) account_name_upper,due_date,amount 
FROM 
`cg-gbq-p.staging_zone.cg_planning_data_main`
)

,final_data as (
select b.account_name,b.cust_id,a.due_date,b.main_class,sum(a.amount) amount from planning_data a
LEFT outer join customer_class b
on upper(a.account_name_upper) = upper(b.account_name_upper)
group by 1,2,3,4
)

select
account_name,
cust_id,
due_date,
main_class,
amount,
current_datetime() as load_date_time
from 
final_data