{{
    config(
        materialized = 'table',
		tags = 'sales_orders_location'	
    )
}}
with sales_orders_location as (
SELECT *
FROM
  cg-gbq-p.oracle_fusion_fscm_partiesanalytics.location
)

select *, current_datetime() as load_date_time from sales_orders_location
