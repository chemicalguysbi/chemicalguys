{{
    config(
        materialized = 'table',
		tags = 'invoice_legal_entity'	
    )
}}

with invoice_legal_entity_col as (
select 
  legal_entity_legal_entity_id  ,
  _fivetran_synced  ,
  legal_entity_activity_code  ,
  legal_entity_created_by  ,
  legal_entity_creation_date  ,
  legal_entity_effective_from  ,
  legal_entity_effective_to  ,
  legal_entity_enterprise_id  ,
  legal_entity_geography_id  ,
  legal_entity_last_update_date  ,
  legal_entity_last_update_login  ,
  legal_entity_last_updated_by  ,
  legal_entity_legal_employer_flag  ,
  legal_entity_legal_entity_identifier  ,
  legal_entity_name  ,
  legal_entity_object_version_number  ,
  legal_entity_parent_psu_id  ,
  legal_entity_party_id  ,
  legal_entity_psu_flag  ,
  legal_entity_sub_activity_code  ,
  legal_entity_transacting_entity_flag  ,
  legal_entity_type_of_company  ,
  _fivetran_deleted  ,
  current_datetime() as load_datetime
  FROM 
  `cg-gbq-p.oracle_fusion_fscm_finextract_xlebiccextract.legal_entity_extract_pvo`
)

select * from invoice_legal_entity_col