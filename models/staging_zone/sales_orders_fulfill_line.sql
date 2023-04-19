
{{
    config(
        materialized = 'table',
		tags = 'sales_orders_fulfill_line'	
    )
}}
with sales_orders_fulfill_line as (
SELECT
*
FROM
  cg-gbq-p.oracle_fusion_fscm_dootop.fulfill_line	
)

select *,current_datetime() as load_date_time from sales_orders_fulfill_line

