
{{
    config(
        materialized = 'table',
		tags = 'sales_orders_fulfill_line'	
    )
}}
with sales_orders_fulfill_line as (
SELECT
  header_sales_channel_code,
  header_salesperson_id,
  header_sold_to_contact_id,
  header_sold_to_customer_id,
  header_sold_to_party_contact_id,
  header_sold_to_party_id,
  header_source_document_type_code,
  header_source_order_system,
  header_source_org_id,
  header_status_code,
  header_submitted_by,
  header_submitted_date,
  header_submitted_flag,
  header_transactional_currency_code,
  line_actual_ship_date,
  line_canceled_flag,
  line_canceled_qty,
  line_category_code,
  line_created_by,
  line_creation_date,
  line_display_line_number,
  line_fulfillment_date,
  line_header_id,
  line_inventory_item_id,
  line_item_type_code,
  line_last_update_date,
  line_last_updated_by,
  line_line_id,
  line_line_number,
  line_on_hold,
  line_open_flag,
  line_ordered_qty,
  line_ordered_uom,
  line_schedule_ship_date,
  line_shipped_qty,
  line_source_line_id,
  line_source_line_number,
  line_source_order_id,
  line_source_order_number,
  line_source_schedule_number,
  line_status_code,
  line_unit_list_price,
  line_unit_selling_price,
  trading_partner_item_tp_item_desc,
  trading_partner_item_tp_item_number,
  _fivetran_deleted
FROM
  cg-gbq-p.oracle_fusion_fscm_dootop.fulfill_line	
)

select *, current_datetime() as load_date_time from sales_orders_fulfill_line

