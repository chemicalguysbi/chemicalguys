{{
    config(
        materialized = 'table',
		tags = 'customer_account_master'	
    )
}}

with customer_account_master_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.oracle_fusion_fscm_partiesanalytics.customer_account`
)

select * from customer_account_master_col