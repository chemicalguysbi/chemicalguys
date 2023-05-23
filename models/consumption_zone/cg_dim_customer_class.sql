{{
    config(
      materialized = 'table',
		  tags = 'cg_dim_customer_class'
    )
}}
with flex_bi_cust_acct_information_vi as(
    SELECT
  s_k_5000,
  _fivetran_deleted,
  _fivetran_synced,
  application_id,
  created_by,
  creation_date,
  cstnr_clsfctn_,
  cstnr_clsfctn_c,
  desc_cstnr_clsfctn_,
  flexfield_code,
  key_cstnr_clsfctn_0,
  last_update_date,
  last_updated_by,
  main_class_c,
  CASE
    WHEN main_class_ = '' THEN 'Others'
  ELSE
  main_class_
END
  AS main_class
FROM
  `cg-gbq-p.oracle_fusion_fscm_custacctinformationbi.flex_bi_cust_acct_information_vi`
)
select 
a.account_name as account_name,
a.account_number as account_number,
a.cust_account_id as customer_account_id,
'DOMESTIC' as customer_type,
104 as customer_class_code,
'UNCATEGORIZED' as customer_category,
b.main_class as main_class,
a.cust_account_id as cust_id,
c.main_class_sub_category as main_class_sub_category,
c.main_class_category as main_class_category,
current_datetime() as load_date_time

from `cg-gbq-p.oracle_fusion_fscm_partiesanalytics.customer_account` a
join flex_bi_cust_acct_information_vi b
on a.cust_account_id = b.s_k_5000
left join `cg-gbq-p.consumption_zone.cg_customer_class_mapping` c
on b.main_class= c.main_class