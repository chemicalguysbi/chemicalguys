{{
    config(
        materialized = 'table',
		tags = 'sales_orders_item_extract'	
    )
}}
with sales_orders_item_extract
 as (
SELECT
  item_base_peoso_transactions_flag,
  item_base_peosource_organization_id,
  item_base_peosource_subinventory,
  item_base_peosource_type,
  item_base_peostart_auto_lot_number,
  item_base_peostart_auto_serial_number,
  item_base_peostart_date_active,
  item_base_peostd_lot_size,
  item_base_peostock_enabled_flag,
  item_base_peostyle_item_flag,
  item_base_peostyle_item_id,
  item_base_peosubcontracting_component,
  item_base_peosubstitution_window_code,
  item_base_peosubstitution_window_days,
  item_base_peotax_code,
  item_base_peotaxable_flag,
  item_base_peotracking_quantity_ind,
  item_base_peotrade_item_descriptor,
  item_base_peoun_number_id,
  item_base_peounder_compl_tolerance_type,
  item_base_peounder_compl_tolerance_value,
  item_base_peounder_return_tolerance,
  item_base_peounder_shipment_tolerance,
  item_base_peounit_height,
  item_base_peounit_length,
  item_base_peounit_of_issue,
  item_base_peounit_volume,
  item_base_peounit_weight,
  item_base_peounit_width,
  item_base_peovariable_lead_time,
  item_base_peovehicle_item_flag,
  item_base_peoversion_id,
  item_base_peovmi_fixed_order_quantity,
  item_base_peovmi_forecast_type,
  item_base_peovmi_maximum_days,
  item_base_peovmi_maximum_units,
  item_base_peovmi_minimum_days,
  item_base_peovmi_minimum_units,
  item_base_peovolume_uom_code,
  item_base_peoweb_status,
  item_base_peoweight_uom_code,
  item_base_peowh_update_date,
  item_base_peowip_supply_locator_id,
  item_base_peowip_supply_subinventory,
  item_base_peowip_supply_type,
  _fivetran_deleted
FROM
  cg-gbq-p.oracle_fusion_fscm_scmextract_egpbiccextract.item_extract_pvo)

  select *, current_datetime() as load_date_time from sales_orders_item_extract
