
{{
    config(
        materialized = 'table',
		tags = 'sales_orders_inventory_org_parameters_cycle_count'	
    )
}}
with sales_orders_inventory_org_parameters_cycle_count as (
SELECT
  organization_id,
  _fivetran_synced,
  business_unit_peobusiness_unit_id,
  business_unit_peolast_update_date,
  legal_entity_peolast_update_date,
  organization_parameter_peolast_update_date,
  schedule_eodeleted_flag,
  schedule_eolast_update_date,
  _fivetran_deleted
FROM
  cg-gbq-p.oracle_fusion_fscm_invorgpublicview.inventory_org_parameters_cycle_count_vcpvo
)

select *, current_datetime() as load_date_time from sales_orders_inventory_org_parameters_cycle_count

