{{
    config(
        materialized = 'table',
		tags = 'cg_v_sales_l3y'	
    )
}}

with cg_v_sales_l3y_col as (
select 
  *,
  current_datetime() as load_datetime
  FROM 
  `cg-gbq-p.lightspeed.v_Sales_L3Y`
)

select * from cg_v_sales_l3y_col