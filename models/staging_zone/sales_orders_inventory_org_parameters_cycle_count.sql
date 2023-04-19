
{{
    config(
        materialized = 'table',
		tags = 'sales_orders_inventory_org_parameters_cycle_count'	
    )
}}
with sales_orders_inventory_org_parameters_cycle_count as (
SELECT*
FROM
  cg-gbq-p.oracle_fusion_fscm_invorgpublicview.inventory_org_parameters_cycle_count_vcpvo
)

select *,current_datetime() as load_date_time from sales_orders_inventory_org_parameters_cycle_count

