
{{
    config(
        materialized = 'table',
		tags = 'sales_orders_header'	
    )
}}
with sales_orders_header as (
SELECT
  *
FROM
  cg-gbq-p.oracle_fusion_fscm_scmextract_doobiccextract.header_extract_pvo
)

select *,current_datetime() as load_date_time from sales_orders_header

