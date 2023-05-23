{{
    config(
        materialized = 'incremental',
        unique_key = 's_key',
		tags = 'invoice_final_fact_test'	
    )
}}

with
  invo_trans_hdr as (
  select distinct
    ra_customer_trx_customer_trx_id as customer_trx_id,
    _fivetran_synced,
    ra_customer_trx_bill_to_customer_id as bill_to_customer_id,
    ra_customer_trx_complete_flag as complete_flag,
    ra_customer_trx_interface_header_attribute_1 as source_order_number,
    ra_customer_trx_interface_header_attribute_3 as order_number,
    ra_customer_trx_invoice_currency_code as invoice_currency_code,
    ra_customer_trx_purchase_order as purchase_order,
    ra_customer_trx_trx_class as trx_class,
    ra_customer_trx_trx_class_lookup_type,
    ra_customer_trx_trx_date as inv_date,
    ra_customer_trx_trx_number as inv_number,
    ra_customer_trx_legal_entity_id as legal_entity_id,
    ra_customer_trx_creation_date,
    _fivetran_deleted,
    load_datetime
  FROM
    `cg-gbq-p.staging_zone.invoice_transaction_header`
  where UPPER(ra_customer_trx_trx_class) = 'INV'
    ),
  inv_trans_line as (
  select distinct
    ra_customer_trx_line_customer_trx_line_id,
    _fivetran_synced,
    ra_customer_trx_line_customer_trx_id,
    ra_customer_trx_line_extended_amount,
    ra_customer_trx_line_inventory_item_id as inventory_item_id,
    load_datetime
  from
    `cg-gbq-p.staging_zone.invoice_transaction_line` ),
  cust_acc_mas as (
  select distinct
    cust_account_id as cust_account_id,
    _fivetran_synced,
    account_name as account_name,
    account_number,
    load_datetime
  from
    `cg-gbq-p.staging_zone.customer_account_master`),
  inv_leg_enti as (
  select distinct
    legal_entity_legal_entity_id as legal_entity_id,
    _fivetran_synced,
    legal_entity_name as business_unit,
    _fivetran_deleted,
    load_datetime
  from
    `cg-gbq-p.staging_zone.invoice_legal_entity`),

  incremental_header as (
  select
    *
  from
    invo_trans_hdr ith 
    where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    invo_trans_hdr ith)
    ),

	increment_line as(
select
    *
  from
    inv_trans_line itl 
    where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    inv_trans_line itl)
    ),
	
	increment_cust_acc as(
select
    *
  from
    cust_acc_mas cam 
    where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    cust_acc_mas cam)
    ),
	
increment_inv_leg as(
select
    *
  from
    inv_leg_enti ile 
    where date(_fivetran_synced) >=(select
    max(date(_fivetran_synced))-3
  from
    inv_leg_enti ile)
    ),
	
	join_cte as (
  select
    ith.ra_customer_trx_creation_date,
    itl.ra_customer_trx_line_extended_amount,
    cam.account_name,
    cam.cust_account_id,
    ith.complete_flag,
    ith.source_order_number,
    ith.order_number,
    ith.invoice_currency_code,
    coalesce(ith.purchase_order,'0') purchase_order,
    ith.trx_class,
    ith.inv_date,
    ith.inv_number,
    coalesce(itl.inventory_item_id,0) inventory_item_id
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
    ),

final_cte as (
select
  sum(join_cte.ra_customer_trx_line_extended_amount ) as inv_total,
  max(join_cte.ra_customer_trx_creation_date )inv_due,
  join_cte.account_name,
  join_cte.cust_account_id,
  join_cte.complete_flag,
  join_cte.source_order_number,
  join_cte.order_number,
  join_cte.invoice_currency_code,
  purchase_order,
  join_cte.trx_class,
  join_cte.inv_date,
  join_cte.inv_number,
  inventory_item_id,
  0 as open_balance
from
  join_cte

group by
 join_cte.account_name,
 join_cte.cust_account_id,
 join_cte.complete_flag,
 join_cte.source_order_number,
 join_cte.order_number,
 join_cte.invoice_currency_code,
 join_cte.purchase_order,
 join_cte.trx_class,
 join_cte.inv_date,
 join_cte.inv_number,
 join_cte.inventory_item_id,
 open_balance
  )
select md5(concat(account_name,
  cust_account_id,
  complete_flag,
  source_order_number,
  order_number,
  invoice_currency_code,
  purchase_order,
  trx_class,
  inv_date,
  inv_number,
  inventory_item_id,
  open_balance)) as s_key,*,current_datetime() as load_datetime
 from final_cte
--  where order_number = '1692860'
