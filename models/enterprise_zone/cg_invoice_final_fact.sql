{{
    config(
        materialized='table',
        unique_key='s_key',
        tags = 'cg_invoice_final_fact'
    )
}}
--incremental load
WITH
  invo_trans_hdr AS (
  SELECT
    ra_customer_trx_customer_trx_id AS customer_trx_id,
    ra_customer_trx_bill_to_customer_id AS bill_to_customer_id,
    ra_customer_trx_complete_flag AS complete_flag,
    ra_customer_trx_creation_date, 
    ra_customer_trx_interface_header_attribute_1 AS SOURCE_ORDER_NUMBER,
    ra_customer_trx_interface_header_attribute_3 AS ORDER_NUMBER,
    ra_customer_trx_invoice_currency_code AS INVOICE_CURRENCY_CODE,
    ra_customer_trx_last_update_date AS LAST_UPDATE_DATE,
    ra_customer_trx_legal_entity_id AS LEGAL_ENTITY_ID,
    ra_customer_trx_purchase_order AS PURCHASE_ORDER,
    ra_customer_trx_trx_class AS TRX_CLASS,
    ra_customer_trx_trx_date AS INV_DATE,
    ra_customer_trx_trx_number AS INV_NUMBER,
    _fivetran_synced

  FROM
    {{ ref('invoice_transaction_header') }}
    WHERE
    UPPER(ra_customer_trx_trx_class)='INV' 
    ),
  inv_trans_line AS (
  SELECT
    ra_customer_trx_line_customer_trx_id , 
    ra_customer_trx_line_extended_amount, 
    ra_customer_trx_line_inventory_item_id AS INVENTORY_ITEM_ID,
    _fivetran_synced
  FROM
     {{ ref('invoice_transaction_line') }} 
    
    ),
  cust_acc_mas AS (
  SELECT
    cust_account_id AS CUST_ACCOUNT_ID,
    account_name AS ACCOUNT_NAME,
    _fivetran_synced
  FROM
   {{ ref('customer_account_master') }}
    
    ),
  inv_leg_enti AS (
  SELECT
    legal_entity_legal_entity_id AS LEGAL_ENTITY_ID,
    
    legal_entity_name AS BUSINESS_UNIT,
    _fivetran_synced

  FROM
     {{ ref('invoice_legal_entity') }}
    
    ),
  join_cte AS (
  SELECT
  ra_customer_trx_line_extended_amount
  ,
  ra_customer_trx_creation_date ,
  ACCOUNT_NAME,
  CUST_ACCOUNT_ID,
  complete_flag,
  SOURCE_ORDER_NUMBER,
  ORDER_NUMBER,
  INVOICE_CURRENCY_CODE,
  COALESCE(PURCHASE_ORDER,'0') as PURCHASE_ORDER,
  trx_class,
  inv_date,
  inv_number,
  COALESCE(inventory_item_id,0)inventory_item_id,
  0 as open_balance
  FROM
    invo_trans_hdr ith
  INNER JOIN
    inv_trans_line itl
  ON
    ith.customer_trx_id = itl.ra_customer_trx_line_customer_trx_id
  INNER JOIN
    cust_acc_mas cam
  ON
    cam.CUST_ACCOUNT_ID = ith.bill_to_customer_id
  INNER JOIN
    inv_leg_enti ile
  ON
    ith.LEGAL_ENTITY_ID = ile.LEGAL_ENTITY_ID
    )
,
final_cte as (
SELECT
  SUM(ra_customer_trx_line_extended_amount ) AS inv_total
  ,
  MAX(ra_customer_trx_creation_date )inv_due
  ,
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
  inventory_item_id,
  open_balance
FROM
  JOIN_CTE

GROUP BY
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
  inventory_item_id,
  open_balance
ORDER BY
  inv_date,
  account_name,
  inv_number ASC
  )
select 
md5(account_name ||cust_account_id ||complete_flag||source_order_number||order_number||invoice_currency_code||purchase_order||
 trx_class ||inv_date||inv_number|| inventory_item_id ||open_balance)s_key,
 * ,current_datetime as load_datetime from final_cte