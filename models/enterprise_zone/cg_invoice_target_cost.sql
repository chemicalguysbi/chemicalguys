{{
    config(
        materialized='incremental',
        unique_key='s_key',
        tags = 'cg_invoice_target_cost'
    )
}}
--delta load cg_final_invoice_target kept filters in underlying cte which goes as "only taking last 3 days data"

with
  invo_trans_hdr as (
  select
    ra_customer_trx_customer_trx_id as customer_trx_id,
    _fivetran_synced,
    ra_customer_trx_address_verification_code,
    ra_customer_trx_agreement_id,
    ra_customer_trx_application_id,
    ra_customer_trx_approval_code,
    ra_customer_trx_batch_id,
    ra_customer_trx_batch_source_seq_id,
    ra_customer_trx_bill_plan_period,
    ra_customer_trx_bill_template_id,
    ra_customer_trx_bill_template_name,
    ra_customer_trx_bill_to_address_id,
    ra_customer_trx_bill_to_contact_id,
    ra_customer_trx_bill_to_customer_id as bill_to_customer_id,
    ra_customer_trx_bill_to_site_use_id,
    ra_customer_trx_billing_date,
    ra_customer_trx_billing_ext_request_id,
    ra_customer_trx_br_amount,
    ra_customer_trx_br_on_hold_flag,
    ra_customer_trx_br_unpaid_flag,
    ra_customer_trx_cc_error_code,
    ra_customer_trx_cc_error_flag,
    ra_customer_trx_cc_error_text,
    ra_customer_trx_comments,
    ra_customer_trx_complete_flag as complete_flag,
    ra_customer_trx_control_completion_reason_code,
    ra_customer_trx_created_by,
    ra_customer_trx_created_from,
    ra_customer_trx_creation_date,
    ra_customer_trx_credit_method_for_installments,
    ra_customer_trx_credit_method_for_rules,
    ra_customer_trx_ct_reference,
    ra_customer_trx_cust_trx_type_seq_id,
    ra_customer_trx_customer_bank_account_id,
    ra_customer_trx_customer_reference,
    ra_customer_trx_customer_reference_date,
    ra_customer_trx_default_tax_exempt_flag,
    ra_customer_trx_default_taxation_country,
    ra_customer_trx_del_contact_email_address,
    ra_customer_trx_delivery_method_code,
    ra_customer_trx_doc_sequence_id,
    ra_customer_trx_doc_sequence_value,
    ra_customer_trx_document_creation_date,
    ra_customer_trx_document_sub_type,
    ra_customer_trx_document_type_id,
    ra_customer_trx_drawee_bank_account_id,
    ra_customer_trx_drawee_contact_id,
    ra_customer_trx_drawee_id,
    ra_customer_trx_drawee_site_use_id,
    ra_customer_trx_exchange_date,
    ra_customer_trx_exchange_rate,
    ra_customer_trx_exchange_rate_type,
    ra_customer_trx_finance_charges,
    ra_customer_trx_first_pty_reg_id,
    ra_customer_trx_fiscal_doc_access_key,
    ra_customer_trx_fiscal_doc_status,
    ra_customer_trx_fob_point,
    ra_customer_trx_initial_customer_trx_id,
    ra_customer_trx_intercompany_flag,
    ra_customer_trx_interest_header_id,
    ra_customer_trx_interface_header_attribute_1 as source_order_number,
    ra_customer_trx_interface_header_attribute_10,
    ra_customer_trx_interface_header_attribute_11,
    ra_customer_trx_interface_header_attribute_12,
    ra_customer_trx_interface_header_attribute_13,
    ra_customer_trx_interface_header_attribute_14,
    ra_customer_trx_interface_header_attribute_15,
    ra_customer_trx_interface_header_attribute_2,
    ra_customer_trx_interface_header_attribute_3 as order_number,
    ra_customer_trx_interface_header_attribute_4,
    ra_customer_trx_interface_header_attribute_5,
    ra_customer_trx_interface_header_attribute_6,
    ra_customer_trx_interface_header_attribute_7,
    ra_customer_trx_interface_header_attribute_8,
    ra_customer_trx_interface_header_attribute_9,
    ra_customer_trx_interface_header_context,
    ra_customer_trx_internal_notes,
    ra_customer_trx_invoice_currency_code as invoice_currency_code,
    ra_customer_trx_invoicing_rule_id,
    ra_customer_trx_last_printed_sequence_num,
    ra_customer_trx_last_update_date as last_update_date,
    ra_customer_trx_last_update_login,
    ra_customer_trx_last_updated_by,
    ra_customer_trx_late_charges_assessed,
    ra_customer_trx_legal_entity_id as legal_entity_id,
    ra_customer_trx_object_version_number,
    ra_customer_trx_old_trx_number,
    ra_customer_trx_org_id,
    ra_customer_trx_orig_system_batch_name,
    ra_customer_trx_override_remit_account_flag,
    ra_customer_trx_paying_customer_id,
    ra_customer_trx_paying_site_use_id,
    ra_customer_trx_payment_attributes,
    ra_customer_trx_payment_server_order_num,
    ra_customer_trx_payment_trxn_extension_id,
    ra_customer_trx_posting_control_id,
    ra_customer_trx_prepayment_flag,
    ra_customer_trx_previous_customer_trx_id,
    ra_customer_trx_primary_resource_salesrep_id,
    ra_customer_trx_print_request_id,
    ra_customer_trx_printing_count,
    ra_customer_trx_printing_last_printed,
    ra_customer_trx_printing_option,
    ra_customer_trx_printing_original_date,
    ra_customer_trx_printing_pending,
    ra_customer_trx_program_application_id,
    ra_customer_trx_program_id,
    ra_customer_trx_program_update_date,
    ra_customer_trx_purchase_order as purchase_order,
    ra_customer_trx_purchase_order_date,
    ra_customer_trx_purchase_order_revision,
    ra_customer_trx_ra_customer_trx_bill_plan_id,
    ra_customer_trx_ready_for_xml_delivery_flag,
    ra_customer_trx_reason_code,
    ra_customer_trx_receipt_method_id,
    ra_customer_trx_recurred_from_trx_number,
    ra_customer_trx_related_batch_source_seq_id,
    ra_customer_trx_related_customer_trx_id,
    ra_customer_trx_remit_bank_acct_use_id,
    ra_customer_trx_remit_to_address_id,
    ra_customer_trx_remit_to_address_seq_id,
    ra_customer_trx_remittance_bank_account_id,
    ra_customer_trx_remittance_batch_id,
    ra_customer_trx_request_id,
    ra_customer_trx_requires_manual_scheduling,
    ra_customer_trx_rev_rec_application,
    ra_customer_trx_reversed_cash_receipt_id,
    ra_customer_trx_set_of_books_id,
    ra_customer_trx_ship_date_actual,
    ra_customer_trx_ship_to_address_id,
    ra_customer_trx_ship_to_contact_id,
    ra_customer_trx_ship_to_customer_id,
    ra_customer_trx_ship_to_party_address_id,
    ra_customer_trx_ship_to_party_contact_id,
    ra_customer_trx_ship_to_party_id,
    ra_customer_trx_ship_to_party_site_use_id,
    ra_customer_trx_ship_to_site_use_id,
    ra_customer_trx_ship_via,
    ra_customer_trx_shipment_id,
    ra_customer_trx_sold_to_contact_id,
    ra_customer_trx_sold_to_customer_id,
    ra_customer_trx_sold_to_party_id,
    ra_customer_trx_sold_to_site_use_id,
    ra_customer_trx_source_document_id,
    ra_customer_trx_source_document_type,
    ra_customer_trx_source_system,
    ra_customer_trx_special_instructions,
    ra_customer_trx_src_invoicing_rule_id,
    ra_customer_trx_status_trx,
    ra_customer_trx_structured_payment_reference,
    ra_customer_trx_term_due_date,
    ra_customer_trx_term_id,
    ra_customer_trx_territory_id,
    ra_customer_trx_third_pty_reg_id,
    ra_customer_trx_trx_business_category,
    ra_customer_trx_trx_class as trx_class,
    ra_customer_trx_trx_class_lookup_type,
    ra_customer_trx_trx_date as inv_date,
    ra_customer_trx_trx_number as inv_number,
    ra_customer_trx_upgrade_method,
    ra_customer_trx_user_defined_fisc_class,
    ra_customer_trx_waybill_number,
    ra_customer_trx_wh_update_date,
    _fivetran_deleted,
    load_datetime
  from
    `cg-gbq-p.staging_zone.invoice_transaction_header`),
  inv_trans_line as (
  select
    ra_customer_trx_line_customer_trx_line_id,
    _fivetran_synced,
    ra_customer_trx_line_accounting_rule_duration,
    ra_customer_trx_line_accounting_rule_id,
    ra_customer_trx_line_acctd_amount_due_original,
    ra_customer_trx_line_acctd_amount_due_remaining,
    ra_customer_trx_line_amount_due_original,
    ra_customer_trx_line_amount_due_remaining,
    ra_customer_trx_line_amount_includes_tax_flag,
    ra_customer_trx_line_assessable_value,
    ra_customer_trx_line_auth_complete_flag,
    ra_customer_trx_line_authorization_number,
    ra_customer_trx_line_autorule_complete_flag,
    ra_customer_trx_line_autorule_duration_processed,
    ra_customer_trx_line_autotax,
    ra_customer_trx_line_bill_plan_line_id,
    ra_customer_trx_line_billing_period_end_date,
    ra_customer_trx_line_billing_period_start_date,
    ra_customer_trx_line_br_adjustment_id,
    ra_customer_trx_line_br_ref_customer_trx_id,
    ra_customer_trx_line_br_ref_payment_schedule_id,
    ra_customer_trx_line_chrg_acctd_amount_remaining,
    ra_customer_trx_line_chrg_amount_remaining,
    ra_customer_trx_line_commercial_discount,
    ra_customer_trx_line_contract_end_date,
    ra_customer_trx_line_contract_line_id,
    ra_customer_trx_line_contract_start_date,
    ra_customer_trx_line_created_by,
    ra_customer_trx_line_creation_date,
    ra_customer_trx_line_customer_trx_id,
    ra_customer_trx_line_default_ussgl_transaction_code,
    ra_customer_trx_line_default_ussgl_trx_code_context,
    ra_customer_trx_line_deferral_exclusion_flag,
    ra_customer_trx_line_description,
    ra_customer_trx_line_doc_line_id_char_1,
    ra_customer_trx_line_doc_line_id_char_2,
    ra_customer_trx_line_doc_line_id_char_3,
    ra_customer_trx_line_doc_line_id_char_4,
    ra_customer_trx_line_doc_line_id_char_5,
    ra_customer_trx_line_doc_line_id_int_1,
    ra_customer_trx_line_doc_line_id_int_2,
    ra_customer_trx_line_doc_line_id_int_3,
    ra_customer_trx_line_doc_line_id_int_4,
    ra_customer_trx_line_doc_line_id_int_5,
    ra_customer_trx_line_extended_acctd_amount,
    ra_customer_trx_line_extended_amount,
    ra_customer_trx_line_fair_market_value_amount,
    ra_customer_trx_line_final_discharge_location_id,
    ra_customer_trx_line_freight_charge,
    ra_customer_trx_line_frt_adj_acctd_remaining,
    ra_customer_trx_line_frt_adj_remaining,
    ra_customer_trx_line_frt_ed_acctd_amount,
    ra_customer_trx_line_frt_ed_amount,
    ra_customer_trx_line_frt_uned_acctd_amount,
    ra_customer_trx_line_frt_uned_amount,
    ra_customer_trx_line_gross_extended_amount,
    ra_customer_trx_line_gross_unit_selling_price,
    ra_customer_trx_line_historical_flag,
    ra_customer_trx_line_initial_customer_trx_line_id,
    ra_customer_trx_line_insurance_charge,
    ra_customer_trx_line_interest_line_id,
    ra_customer_trx_line_interface_line_attribute_1,
    ra_customer_trx_line_interface_line_attribute_10,
    ra_customer_trx_line_interface_line_attribute_11,
    ra_customer_trx_line_interface_line_attribute_12,
    ra_customer_trx_line_interface_line_attribute_13,
    ra_customer_trx_line_interface_line_attribute_14,
    ra_customer_trx_line_interface_line_attribute_15,
    ra_customer_trx_line_interface_line_attribute_2,
    ra_customer_trx_line_interface_line_attribute_3,
    ra_customer_trx_line_interface_line_attribute_4,
    ra_customer_trx_line_interface_line_attribute_5,
    ra_customer_trx_line_interface_line_attribute_6,
    ra_customer_trx_line_interface_line_attribute_7,
    ra_customer_trx_line_interface_line_attribute_8,
    ra_customer_trx_line_interface_line_attribute_9,
    ra_customer_trx_line_interface_line_context,
    ra_customer_trx_line_inventory_item_id as inventory_item_id,
    ra_customer_trx_line_invoiced_line_acctg_level,
    ra_customer_trx_line_item_context,
    ra_customer_trx_line_item_exception_rate_id,
    ra_customer_trx_line_last_period_to_credit,
    ra_customer_trx_line_last_update_date,
    ra_customer_trx_line_last_update_login,
    ra_customer_trx_line_last_updated_by,
    ra_customer_trx_line_line_intended_use,
    ra_customer_trx_line_line_number,
    ra_customer_trx_line_line_recoverable,
    ra_customer_trx_line_line_type,
    ra_customer_trx_line_link_to_cust_trx_line_id,
    ra_customer_trx_line_link_to_parentline_attribute_1,
    ra_customer_trx_line_link_to_parentline_attribute_10,
    ra_customer_trx_line_link_to_parentline_attribute_11,
    ra_customer_trx_line_link_to_parentline_attribute_12,
    ra_customer_trx_line_link_to_parentline_attribute_13,
    ra_customer_trx_line_link_to_parentline_attribute_14,
    ra_customer_trx_line_link_to_parentline_attribute_15,
    ra_customer_trx_line_link_to_parentline_attribute_2,
    ra_customer_trx_line_link_to_parentline_attribute_3,
    ra_customer_trx_line_link_to_parentline_attribute_4,
    ra_customer_trx_line_link_to_parentline_attribute_5,
    ra_customer_trx_line_link_to_parentline_attribute_6,
    ra_customer_trx_line_link_to_parentline_attribute_7,
    ra_customer_trx_line_link_to_parentline_attribute_8,
    ra_customer_trx_line_link_to_parentline_attribute_9,
    ra_customer_trx_line_link_to_parentline_context,
    ra_customer_trx_line_location_segment_id,
    ra_customer_trx_line_memo_line_seq_id,
    ra_customer_trx_line_miscellaneous_charge,
    ra_customer_trx_line_movement_id,
    ra_customer_trx_line_object_version_number,
    ra_customer_trx_line_org_id,
    ra_customer_trx_line_override_auto_accounting_flag,
    ra_customer_trx_line_packing_charge,
    ra_customer_trx_line_payment_set_id,
    ra_customer_trx_line_payment_trxn_extension_id,
    ra_customer_trx_line_previous_customer_trx_id,
    ra_customer_trx_line_previous_customer_trx_line_id,
    ra_customer_trx_line_product_category,
    ra_customer_trx_line_product_fisc_classification,
    ra_customer_trx_line_product_type,
    ra_customer_trx_line_program_application_id,
    ra_customer_trx_line_program_id,
    ra_customer_trx_line_program_update_date,
    ra_customer_trx_line_quantity_credited,
    ra_customer_trx_line_quantity_invoiced as quantity,
    ra_customer_trx_line_quantity_ordered,
    ra_customer_trx_line_ra_customer_trx_line_unit_selling_price,
    ra_customer_trx_line_reason_code,
    ra_customer_trx_line_recurring_bill_flag,
    ra_customer_trx_line_recurring_bill_plan_id,
    ra_customer_trx_line_recurring_bill_plan_line_id,
    ra_customer_trx_line_request_id,
    ra_customer_trx_line_requires_manual_scheduling,
    ra_customer_trx_line_revenue_amount,
    ra_customer_trx_line_rule_end_date,
    ra_customer_trx_line_rule_start_date,
    ra_customer_trx_line_sales_order,
    ra_customer_trx_line_sales_order_date,
    ra_customer_trx_line_sales_order_line,
    ra_customer_trx_line_sales_order_source,
    ra_customer_trx_line_set_of_books_id,
    ra_customer_trx_line_ship_to_address_id,
    ra_customer_trx_line_ship_to_contact_id,
    ra_customer_trx_line_ship_to_customer_id,
    ra_customer_trx_line_ship_to_party_address_id,
    ra_customer_trx_line_ship_to_party_contact_id,
    ra_customer_trx_line_ship_to_party_id,
    ra_customer_trx_line_ship_to_party_site_use_id,
    ra_customer_trx_line_ship_to_site_use_id,
    ra_customer_trx_line_source_data_key_1,
    ra_customer_trx_line_source_data_key_2,
    ra_customer_trx_line_source_data_key_3,
    ra_customer_trx_line_source_data_key_4,
    ra_customer_trx_line_source_data_key_5,
    ra_customer_trx_line_source_document_line_id,
    ra_customer_trx_line_source_document_line_number,
    ra_customer_trx_line_tax_action,
    ra_customer_trx_line_tax_classification_code,
    ra_customer_trx_line_tax_exempt_flag,
    ra_customer_trx_line_tax_exempt_number,
    ra_customer_trx_line_tax_exempt_reason_code,
    ra_customer_trx_line_tax_exemption_id,
    ra_customer_trx_line_tax_invoice_date,
    ra_customer_trx_line_tax_invoice_number,
    ra_customer_trx_line_tax_line_id,
    ra_customer_trx_line_tax_precedence,
    ra_customer_trx_line_tax_rate,
    ra_customer_trx_line_tax_recoverable,
    ra_customer_trx_line_tax_vendor_return_code,
    ra_customer_trx_line_taxable_amount,
    ra_customer_trx_line_translated_description,
    ra_customer_trx_line_trx_business_category,
    ra_customer_trx_line_unit_standard_price,
    ra_customer_trx_line_uom_code,
    ra_customer_trx_line_user_defined_fisc_class,
    ra_customer_trx_line_vat_tax_id,
    ra_customer_trx_line_warehouse_id,
    ra_customer_trx_line_wh_update_date,
    _fivetran_deleted,
    load_datetime
  from
    `cg-gbq-p.staging_zone.invoice_transaction_line` ),
  cust_acc_mas as (
  select
    cust_account_id as cust_account_id,
    _fivetran_synced,
    account_established_date,
    account_name as account_name,
    account_number,
    account_termination_date,
    arrivalsets_include_lines_flag,
    autopay_flag,
    comments,
    coterminate_day_month,
    created_by,
    created_by_module,
    creation_date,
    customer_class_code,
    customer_type,
    date_type_preference,
    deposit_refund_method,
    held_bill_expiration_date,
    hold_bill_flag,
    last_batch_id,
    last_update_date,
    last_update_login,
    last_updated_by,
    npa_number,
    orig_system_reference,
    party_id,
    selling_party_id,
    source_code,
    status,
    status_update_date,
    tax_code,
    tax_header_level_flag,
    tax_rounding_rule,
    _fivetran_deleted,
    load_datetime
  from
    `cg-gbq-p.staging_zone.customer_account_master`),
  inv_leg_enti as (
  select
    legal_entity_legal_entity_id as legal_entity_id,
    _fivetran_synced,
    legal_entity_activity_code,
    legal_entity_created_by,
    legal_entity_creation_date,
    legal_entity_effective_from,
    legal_entity_effective_to,
    legal_entity_enterprise_id,
    legal_entity_geography_id,
    legal_entity_last_update_date,
    legal_entity_last_update_login,
    legal_entity_last_updated_by,
    legal_entity_legal_employer_flag,
    legal_entity_legal_entity_identifier,
    legal_entity_name as business_unit,
    legal_entity_object_version_number,
    legal_entity_parent_psu_id,
    legal_entity_party_id,
    legal_entity_psu_flag,
    legal_entity_sub_activity_code,
    legal_entity_transacting_entity_flag,
    legal_entity_type_of_company,
    _fivetran_deleted,
    load_datetime
  from
    `cg-gbq-p.staging_zone.invoice_legal_entity`),
  standard_cost as (
  select
    organization_id,
    inventory_item_id as inventory_itemid,
    organiztion_code,
    cst_org_name,
    effective_start_date,
    effective_end_date,
    std_cost
  from
    `cg-gbq-p.staging_zone.cg_item_standard_cost` 
  where
    effective_start_date<=current_date
    and effective_end_date>=current_date ),
	


incremental_header as (
  select
    *
  from
    invo_trans_hdr ith where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    invo_trans_hdr ith))
    --select count(*) from incremental_header
    
    ,
	increment_line as(
select
    *
  from
    inv_trans_line itl where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    inv_trans_line itl))
   --select count(*) from  increment_line
    ,
	
	increment_cust_acc as(
select
    *
  from
    cust_acc_mas cam where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    cust_acc_mas cam)),
	
increment_inv_leg as(
select
    *
  from
    inv_leg_enti ile where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    inv_leg_enti ile))	,
	

  join_cte as (
  select
    *
  from
    incremental_header ith
  inner join
    increment_line itl
  on
    ith.customer_trx_id = itl.ra_customer_trx_line_customer_trx_id
  inner join
    increment_cust_acc cam
  on
    cam.cust_account_id = ith.bill_to_customer_id
  inner join
    increment_inv_leg ile
  on
    ith.legal_entity_id = ile.legal_entity_id
  right join
    standard_cost sc
  on
    itl.ra_customer_trx_line_warehouse_id = sc.organization_id
    and sc.inventory_itemid = itl.inventory_item_id
  where
    upper(ith.trx_class)='inv'),
  --112996408 
  aggregated_data as (
  select
    distinct join_cte.account_name,
    join_cte.cust_account_id,
    join_cte.complete_flag,
    join_cte.source_order_number,
    join_cte.order_number,
    join_cte.invoice_currency_code,
    join_cte.purchase_order,
    join_cte.trx_class,
    join_cte.inv_date,
    join_cte.inv_number,
    join_cte.inventory_itemid,
    join_cte.std_cost * join_cte.quantity as standard_cost,
    join_cte.quantity,
    join_cte.ra_customer_trx_line_extended_amount,
    join_cte.ra_customer_trx_billing_date
  from
    join_cte ),
  cg_final_invoice_target as (
  select
    sum(aggregated_data.ra_customer_trx_line_extended_amount ) as inv_total,
    max(aggregated_data.ra_customer_trx_billing_date )inv_due,
    aggregated_data.inventory_itemid as inventory_item_id,
    aggregated_data.account_name,
    aggregated_data.cust_account_id,
    aggregated_data.complete_flag,
    aggregated_data.source_order_number,
    aggregated_data.order_number,
    aggregated_data.invoice_currency_code,
    coalesce(aggregated_data.purchase_order,'0') as purchase_order,
    aggregated_data.trx_class,
    aggregated_data.inv_date,
    aggregated_data.inv_number,
    sum(aggregated_data.standard_cost)standard_cost,
    sum(aggregated_data.quantity)quantity
  from
    aggregated_data
  group by
    aggregated_data.account_name,
    aggregated_data.cust_account_id,
    aggregated_data.complete_flag,
    aggregated_data.source_order_number,
    aggregated_data.order_number,
    aggregated_data.invoice_currency_code,
    aggregated_data.purchase_order,
    aggregated_data.trx_class,
    aggregated_data.inv_date,
    aggregated_data.inv_number,
    aggregated_data.inventory_itemid 
    --aggregated_data.standard_cost,
    --aggregated_data.quantity
  order by
    aggregated_data.inv_date,
    aggregated_data.account_name,
    aggregated_data.inv_number asc ) --data 5161270
select
  md5(account_name ||cust_account_id ||complete_flag||source_order_number||order_number||invoice_currency_code||purchase_order|| trx_class ||inv_date||inv_number|| inventory_item_id)s_key,
  *,
  current_datetime() as load_datetime
from
  cg_final_invoice_target