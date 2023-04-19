{{
    config(
        materialized = 'table',
		tags = 'invoice_payment_schedule'	
    )
}}

with invoice_payment_schedule_col as (
select 
  *,
  current_datetime() as load_datetime
  
  from 
  `cg-gbq-p.oracle_fusion_fscm_finextract_arbiccextract.payment_schedule_extract_pvo`)
  
  select * from invoice_payment_schedule_col