{{
    config(
      materialized = 'table',
		  tags = 'cg_dim_customer_class'
    )
}}

select 
a.account_name as account_name,
a.account_number as account_number,
a.cust_account_id as customer_account_id,
'DOMESTIC' as customer_type,
104 as customer_class_code,
'UNCATEGORIZED' as customer_category,
b.main_class_ as main_class,
a.cust_account_id as cust_id,
c.main_class_sub_category as main_class_sub_category,
c.main_class_category as main_class_category,
current_datetime() as load_date_time

from `cg-gbq-p.oracle_fusion_fscm_partiesanalytics.customer_account` a
join `cg-gbq-p.oracle_fusion_fscm_custacctinformationbi.flex_bi_cust_acct_information_vi` b
on a.cust_account_id = b.s_k_5000
join `cg-gbq-p.consumption_zone.cg_customer_class_mapping` c
on b.main_class_= c.main_class