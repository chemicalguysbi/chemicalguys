{{
    config(
        materialized = 'table',
		tags = 'cg_item_master'	
    )
}}

with cg_item_master_col as (
select *,current_date as load_datetime from 
`cg-gbq-p.oracle_nets.item_master`)
select * from cg_item_master_col