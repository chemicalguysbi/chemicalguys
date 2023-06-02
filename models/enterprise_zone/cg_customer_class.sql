{{
    config(
        materialized = 'table',
		tags = 'cg_customer_class'	
    )
}}

with cg_customer_class_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.staging_zone.cg_customer_class`
)

select * from cg_customer_class_col