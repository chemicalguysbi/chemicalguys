{{
    config(
        materialized = 'table',
		tags = 'cg_inventory_on_hand'	
    )
}}

with inventory_on_hand_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.oracle_nets.inventory_on_hand`
)

select * from inventory_on_hand_col