with source_data as (
select * from (SELECT 
header_id,
_fivetran_synced as fivetran_synced,			
header_agreement_header_id as agreement_header,			
header_agreement_version_number,			
header_allow_currency_override_flag
 FROM `cg-gbq-p.oracle_fusion_fscm_scmextract_doobiccextract.header_extract_pvo` LIMIT 10) a cross join

 (SELECT  
ar_payment_schedule_payment_schedule_id,		
_fivetran_synced,	
ar_payment_schedule_acctd_amount_due_remaining,		
ar_payment_schedule_active_claim_flag	,			
ar_payment_schedule_actual_date_closed,			
ar_payment_schedule_amount_adjusted

FROM `cg-gbq-p.oracle_fusion_fscm_finextract_arbiccextract.payment_schedule_extract_pvo` 
limit 10) b
)
select *,current_datetime() as load_date_time
from source_data