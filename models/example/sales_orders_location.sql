{{
    config(
        materialized = 'table',
		tags = 'sales_orders_location'	
    )
}}
with sales_orders_location as (
SELECT
  order_address_id,
  _fivetran_synced,
  order_address_contact_id,
  order_address_created_by,
  order_address_creation_date,
  order_address_cust_account_contact_id,
  order_address_cust_acct_id,
  order_address_cust_acct_site_use_id
FROM
  cg-gbq-p.oracle_fusion_fscm_scmextract_doobiccextract.order_address_extract_pvo
)

select *, current_datetime() as load_date_time from sales_orders_location
