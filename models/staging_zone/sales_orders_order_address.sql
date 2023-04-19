{{
    config(
        materialized = 'table',
		tags = 'sales_orders_order_address'	
    )
}}
with sales_orders_order_address as (
SELECT *
FROM
  cg-gbq-p.oracle_fusion_fscm_scmextract_doobiccextract.order_address_extract_pvo
)

  select *, current_datetime() as load_date_time from sales_orders_order_address
