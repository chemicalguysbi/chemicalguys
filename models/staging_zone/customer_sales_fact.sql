{{
    config(
        materialized = 'table',
		tags = 'customer_sales_fact'	
    )
}}

with customer_sales_fact_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.lightspeed.customer_sales_fact`
)

select * from customer_sales_fact_col