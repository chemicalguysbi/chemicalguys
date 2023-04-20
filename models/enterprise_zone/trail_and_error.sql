{{
    config(
        materialized = 'table',
		tags = 'trail_and_error'
    )
}}

with customer_account_master_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.oracle_fusion_fscm_partiesanalytics.customer_account`
)

select * from customer_account_master_col