{{
    config(
        materialized = 'table',
		tags = 'invoice_legal_entity'	
    )
}}

with invoice_legal_entity_col as (
select 
  *,
  current_datetime() as load_datetime
  FROM 
  `cg-gbq-p.oracle_fusion_fscm_finextract_xlebiccextract.legal_entity_extract_pvo`
)

select * from invoice_legal_entity_col