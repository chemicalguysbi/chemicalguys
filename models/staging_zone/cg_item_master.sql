{{
    config(
        materialized = 'table',
		tags = 'cg_item_master'	
    )
}}

with cg_item_master_col as (
select case when length(SKU)< 8 then SKU 
WHEN LENGTH(SKU)>=8 AND REGEXP_CONTAINS(SKU, r".*_.*") = true THEN SUBSTRING(SKU,1,7) 
when length(SKU) >=8 AND REGEXP_CONTAINS(SKU, r".*_.*") = false then SUBSTRING(SKU,1,6)

else null end as parent_sku,*,current_date as load_datetime from 
`cg-gbq-p.oracle_nets.item_master`)
select * from cg_item_master_col