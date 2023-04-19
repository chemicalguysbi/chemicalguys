{{
    config(
        materialized = 'table',
		tags = 'sales_orders_item_extract'	
    )
}}
with sales_orders_item_extract
 as (
SELECT *
FROM
  cg-gbq-p.oracle_fusion_fscm_scmextract_egpbiccextract.item_extract_pvo)

  select *,current_datetime() as load_date_time from sales_orders_item_extract
