{{
    config(
        materialized = 'table',
		tags = 'customer_account_master'	
    )
}}

with customer_account_master_col as (
select 
  cust_account_id  ,
  _fivetran_synced  ,
  account_established_date  ,
  account_name  ,
  account_number  ,
  account_termination_date  ,
  arrivalsets_include_lines_flag  ,
  autopay_flag  ,
  comments  ,
  coterminate_day_month  ,
  created_by  ,
  created_by_module  ,
  creation_date  ,
  customer_class_code  ,
  customer_type  ,
  date_type_preference  ,
  deposit_refund_method  ,
  held_bill_expiration_date  ,
  hold_bill_flag  ,
  last_batch_id  ,
  last_update_date  ,
  last_update_login  ,
  last_updated_by  ,
  npa_number  ,
  orig_system_reference  ,
  party_id  ,
  selling_party_id  ,
  source_code  ,
  status  ,
  status_update_date  ,
  tax_code  ,
  tax_header_level_flag  ,
  tax_rounding_rule  ,
  _fivetran_deleted  ,
  current_datetime() as load_datetime
  from `cg-gbq-p.oracle_fusion_fscm_partiesanalytics.customer_account`
)

select * from customer_account_master_col