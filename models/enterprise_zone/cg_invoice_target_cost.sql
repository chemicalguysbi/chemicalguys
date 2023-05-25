{{
    config(
        materialized='table',
        tags = 'cg_invoice_target_cost'
    )
}}
--delta load cg_final_invoice_target kept filters in underlying cte which goes as "only taking last 3 days data"

WITH
  invo_trans_hdr AS (
  SELECT distinct
    ra_customer_trx_customer_trx_id as customer_trx_id,
    ra_customer_trx_bill_to_customer_id as bill_to_customer_id,
    ra_customer_trx_complete_flag as complete_flag,
    ra_customer_trx_interface_header_attribute_1 as source_order_number,
    ra_customer_trx_interface_header_attribute_3 as order_number,
    ra_customer_trx_invoice_currency_code as invoice_currency_code,
    ra_customer_trx_last_update_date as last_update_date,
    ra_customer_trx_legal_entity_id as legal_entity_id,
    ra_customer_trx_purchase_order as purchase_order,
    ra_customer_trx_trx_class as trx_class,
    ra_customer_trx_trx_date as inv_date,
    ra_customer_trx_trx_number as inv_number,
    ra_customer_trx_billing_date
  FROM
    {{ ref('invoice_transaction_header') }}
    where UPPER(ra_customer_trx_trx_class) = 'INV'
    
    ),
 inv_trans_line AS (
  SELECT distinct
    ra_customer_trx_line_inventory_item_id as inventory_item_id,
    ra_customer_trx_line_quantity_invoiced as quantity,
    ra_customer_trx_line_customer_trx_id,
    ra_customer_trx_line_warehouse_id,
    ra_customer_trx_line_extended_amount,
  
  FROM
    {{ ref('invoice_transaction_line') }} 
    )
	
  ,cust_acc_mas AS (
  SELECT distinct
    cust_account_id as cust_account_id,
    account_name as account_name

  FROM
    {{ ref('customer_account_master') }} 
    )

  ,inv_leg_enti AS (
  SELECT distinct
    legal_entity_legal_entity_id as legal_entity_id,
    legal_entity_name as business_unit
  FROM
    {{ ref('invoice_legal_entity') }}
    )
--   ,standard_cost AS (
--   SELECT distinct
--     organization_id,
--     inventory_item_id AS inventory_itemid,
--     organization_code,
--     cst_org_name,
--     effective_start_date,
--     effective_end_date,
--     std_cost
--   FROM
--     `cg-gbq-p.staging_zone.cg_item_standard_cost`
--   WHERE
--     effective_start_date<=current_date
--     AND effective_end_date>=current_date )
	

  ,join_cte AS (
  SELECT
    cam.account_name,
    cam.cust_account_id,
    ith.complete_flag,
    ith.source_order_number,
    ith.order_number,
    ith.invoice_currency_code,
    coalesce(ith.purchase_order,'0')purchase_order,
    ith.trx_class,
    ith.inv_date,
    ith.inv_number,
    COALESCE(itl.inventory_item_id,0) as inventory_itemid,
    COALESCE(round(sc.current_cost * itl.quantity),0) AS standard_cost,
    coalesce(itl.quantity,0) quantity,
    itl.ra_customer_trx_line_extended_amount,
    ith.ra_customer_trx_billing_date,

 
  from
    invo_trans_hdr ith
  inner join
    inv_trans_line itl
  on
    ith.customer_trx_id = itl.ra_customer_trx_line_customer_trx_id
  inner join
    cust_acc_mas cam
  on
    cam.cust_account_id = ith.bill_to_customer_id
  inner join
    inv_leg_enti ile
  on
    ith.legal_entity_id = ile.legal_entity_id
  left join
    {{ ref('standard_cost') }} sc
  on
    itl.ra_customer_trx_line_warehouse_id = sc.organization_id
    and sc.inventory_item_id = itl.inventory_item_id
  )
 
  select
    sum(ra_customer_trx_line_extended_amount ) as inv_total,
    max(ra_customer_trx_billing_date )inv_due,
    inventory_itemid,
    account_name,
    cust_account_id,
    complete_flag,
    source_order_number,
    order_number,
    invoice_currency_code,
    purchase_order,
    trx_class,
    inv_date,
    inv_number,
    standard_cost,
    quantity
  from
    join_cte
  group by
    account_name,
    cust_account_id,
    complete_flag,
    source_order_number,
    order_number,
    invoice_currency_code,
    purchase_order,
    trx_class,
    inv_date,
    inv_number,
    inventory_itemid ,
    standard_cost,
    quantity