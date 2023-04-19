{{
    config(
        materialized = 'table',
		tags = 'invoice_transaction_header'	
    )
}}

with invoice_transaction_header_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.oracle_fusion_fscm_finextract_arbiccextract.transaction_header_extract_pvo`
  )
  select * from invoice_transaction_header_col