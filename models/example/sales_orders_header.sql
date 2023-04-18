
{{
    config(
        materialized = 'table',
		tags = 'sales_orders_header'	
    )
}}
with sales_orders_header as (
SELECT
  header_partial_ship_allowed_flag,
  header_payment_term_id,
  header_payment_term_id_derived,
  header_pre_credit_checked_flag,
  header_priced_on,
  header_pricing_segment_code,
  header_pricing_segment_explanation,
  header_pricing_strategy_explanation,
  header_pricing_strategy_id,
  header_reference_header_id,
  header_request_arrival_date,
  header_request_cancel_date,
  header_request_ship_date,
  header_revision_source_order_system,
  header_sales_channel_code,
  header_salesperson_id,
  header_segment_explanation_msg_name,
  header_ship_class_of_service,
  header_ship_mode_of_transport,
  header_shipment_priority_code,
  header_shipping_instructions,
  header_shipset_flag,
  header_sold_to_contact_id,
  header_sold_to_customer_id,
  header_sold_to_party_contact_id,
  header_sold_to_party_contact_point_id,
  header_sold_to_party_id,
  header_source_document_type_code,
  header_source_order_id,
  header_source_order_number,
  header_source_order_system,
  header_source_org_id,
  header_source_revision_number,
  header_status_code,
  header_strategy_explanation_msg_name,
  header_subinventory,
  header_submitted_by,
  header_submitted_date,
  header_submitted_flag,
  header_substitute_allowed_flag,
  header_supplier_id,
  header_supplier_site_id,
  header_trade_compliance_result_code,
  header_transactional_currency_code,
  header_transactional_currency_code_derived,
  _fivetran_deleted
FROM
  cg-gbq-p.oracle_fusion_fscm_scmextract_doobiccextract.header_extract_pvo
)

select *, current_datetime() as load_date_time from sales_orders_header

