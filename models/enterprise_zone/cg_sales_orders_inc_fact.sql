{{
    config(
        materialized='table',
        unique_key = 's_key',
        tags = 'cg_sales_orders_inc_fact'
    )
}}
--CG_DOO_FULFILL_LINES_ALL
with sales_orders_fulfill_line_cte as (
SELECT distinct
fulfill_line_source_order_number	as	source_order_number	,
fulfill_line_creation_date	as	order_date	,
fulfill_line_request_ship_date	as	request_ship_date	,
fulfill_line_source_line_number	as	source_line_number	,
fulfill_line_fulfill_line_number as	fulfill_line_number	,
fulfill_line_id	as fulfill_line_id	,
fulfill_line_ordered_qty as ordered_qty_ea	,
fulfill_line_unit_selling_price	as	ea_price	,
fulfill_line_status_code as	line_status	,
fulfill_line_ship_to_party_site_id	as	ship_to_party_site_id	,
fulfill_line_header_id	as	header_id	,
fulfill_line_fulfill_org_id	as	fulfill_org_id	,
fulfill_line_inventory_item_id	as	inventory_item_id	

FROM
 
{{ ref('sales_orders_fulfill_line') }}
--CG_DOO_HEADERS_ALL
,sales_orders_header_cte as (
  SELECT distinct
  header_id AS header_id,
  header_open_flag AS open_flag,
  header_ordered_date AS ordered_date,
  header_order_number as order_number,
  header_source_order_number AS source_order_number,
  header_source_revision_number AS source_revision_number,
  header_status_code AS status_code,
  header_customer_po_number AS customer_po_number,
  header_request_ship_date AS request_ship_date,
  header_submitted_flag AS submitted_flag,
  header_change_version_number AS change_revision_number
FROM
  {{ ref('sales_orders_header') }}
  where (header_status_code <> 'DOO_DRAFT' AND header_submitted_flag = 'Y') OR
  (header_status_code = 'DOO_DRAFT' AND header_customer_po_number = '1')
)
--CG_INV_ORGANIZATION_DEFINITIONS_V
,sales_orders_inventory_org_parameters_cycle_count_cte as (
  SELECT distinct
business_unit_peobusiness_unit_id as organization_code,
organization_id
FROM
 {{ ref('sales_orders_inventory_org_parameters_cycle_count') }}
)
--CG_EGP_SYSTEM_ITEMS_B
,sales_orders_item_extract_cte as (
  SELECT distinct
item_base_peoitem_number as item_number,
item_base_peoinventory_item_id as inventory_item_id,
item_base_peoorganization_id as organization_id,
item_base_peotrade_item_descriptor as description

FROM
 {{ ref('sales_orders_item_extract') }}
)
--CG_DOO_ORDER_ADDRESSES
,sales_orders_order_address_cte as (
  SELECT distinct
order_address_party_site_id as party_site_id,
order_address_header_id as header_id,
order_address_cust_acct_id as cust_acct_id,
order_address_use_type as address_use_type

FROM
 {{ ref('sales_orders_order_address') }}
)

--CG_DOO_ORDER_ADDRESSES_2
,sales_orders_order_address_cte_2 as (
  SELECT distinct
order_address_header_id as header_id,
order_address_cust_acct_id as cust_acct_id,
order_address_use_type as address_use_type

FROM
  {{ ref('sales_orders_order_address') }}
)

--CG_HZ_LOCATIONS
,sales_orders_location_cte as (
  select distinct
  address_1,
  address_2,
  city,
  state,
  country,
  postal_code,
  location_id

  from
  {{ ref('sales_orders_location') }}
)

--CG_HZ_PARTY_SITES
,sales_orders_party_site_cte as (
  SELECT distinct
party_site_id,
location_id,
'SHIP_TO' as site_use_type

FROM
 {{ ref('sales_orders_party_site') }}
)

--CG_HZ_CUST_ACCOUNTS
,customer_account_master_cte as (
  select distinct 
  account_name,
  cust_account_id
   from 
   {{ ref('customer_account_master') }}
)

,final_cte as (
  select 

sales_orders_fulfill_line_cte.order_date,
sales_orders_fulfill_line_cte.fulfill_line_number,
sales_orders_fulfill_line_cte.fulfill_org_id,
sales_orders_fulfill_line_cte.fulfill_line_id,
sales_orders_fulfill_line_cte.ordered_qty_ea,
sales_orders_fulfill_line_cte.ship_to_party_site_id,
sales_orders_fulfill_line_cte.source_line_number,
sales_orders_fulfill_line_cte.line_status,  
sales_orders_fulfill_line_cte.ea_price,
sales_orders_header_cte.customer_po_number,
sales_orders_header_cte.open_flag,
sales_orders_header_cte.order_number,
sales_orders_header_cte.ordered_date,
sales_orders_header_cte.source_order_number,
sales_orders_header_cte.source_revision_number,
sales_orders_header_cte.status_code,
sales_orders_inventory_org_parameters_cycle_count_cte.organization_code,
sales_orders_item_extract_cte.inventory_item_id,
sales_orders_item_extract_cte.item_number,
sales_orders_item_extract_cte.description,
sales_orders_order_address_cte.party_site_id,
'ship_to' as site_use_type,

sales_orders_location_cte.address_1,
sales_orders_location_cte.address_2,
sales_orders_location_cte.city,
sales_orders_location_cte.country,
sales_orders_location_cte.postal_code,
sales_orders_location_cte.state,

sales_orders_order_address_cte_2.cust_acct_id,
customer_account_master_cte.account_name,

  coalesce(sales_orders_header_cte.request_ship_date,sales_orders_fulfill_line_cte.request_ship_date) as 
  request_ship_date,
--   (safe_cast(sales_orders_fulfill_line_cte.ordered_qty_ea as float64) * safe_cast(sales_orders_fulfill_line_cte.EA_PRICE as FLOAT64)) as
--   extended_amount
  round((sales_orders_fulfill_line_cte.ordered_qty_ea * sales_orders_fulfill_line_cte.EA_PRICE),2) extended_amount
   from sales_orders_fulfill_line_cte
---join 1  7081375
inner join sales_orders_header_cte 
on sales_orders_fulfill_line_cte.source_order_number = sales_orders_header_cte.source_order_number 
AND sales_orders_fulfill_line_cte.header_id = sales_orders_header_cte.header_id
--join 2 7081375
inner join sales_orders_inventory_org_parameters_cycle_count_cte
ON sales_orders_fulfill_line_cte.fulfill_org_id = sales_orders_inventory_org_parameters_cycle_count_cte.organization_id
-- ---join 3 7081375
inner join sales_orders_item_extract_cte
ON sales_orders_fulfill_line_cte.inventory_item_id = sales_orders_item_extract_cte.inventory_item_id AND sales_orders_fulfill_line_cte.fulfill_org_id = sales_orders_item_extract_cte.organization_id

-- ---join 4 7108888
inner join sales_orders_order_address_cte
ON sales_orders_header_cte.header_id = sales_orders_order_address_cte.header_id AND sales_orders_fulfill_line_cte.ship_to_party_site_id = sales_orders_order_address_cte.party_site_id

-- ---join 6 7108888
inner join sales_orders_party_site_cte
on sales_orders_order_address_cte.party_site_id = sales_orders_party_site_cte.party_site_id

-- --join 7 
inner join sales_orders_location_cte
on sales_orders_party_site_cte.location_id = sales_orders_location_cte.location_id

-- ---join 9  14426762
inner join sales_orders_order_address_cte_2 
ON sales_orders_fulfill_line_cte.header_id = sales_orders_order_address_cte_2.HEADER_ID 

-- --join 10  7213375
inner join customer_account_master_cte 
on sales_orders_order_address_cte_2.cust_acct_id = customer_account_master_cte.cust_account_id

where 

-- sales_orders_header_cte.open_flag = 'Y' AND
sales_orders_item_extract_cte.item_number <> 'Discount' AND sales_orders_item_extract_cte.item_number <> 'Discount 5' AND sales_orders_item_extract_cte.item_number <> 'Discount 10' AND sales_orders_item_extract_cte.item_number <> 'FREIGHT CHARGE' AND sales_orders_item_extract_cte.item_number <> 'DISCOUNT CHARGE' AND sales_orders_item_extract_cte.item_number <> 'HANDLING CHARGE'
and sales_orders_order_address_cte_2.address_use_type = 'BILL_TO'
)

,aggregrate_cte as (
  select 
  order_date,
fulfill_line_number,
fulfill_org_id,
fulfill_line_id,
ordered_qty_ea,
ship_to_party_site_id,
source_line_number,
line_status,  
ea_price,
customer_po_number,
open_flag,
order_number,
ordered_date,
source_order_number,
source_revision_number,
status_code,
organization_code,
inventory_item_id,
item_number,
description,
party_site_id,
site_use_type,
address_1,
address_2,
city,
country,
postal_code,
state,
cust_acct_id,
account_name,
request_ship_date,
sum(extended_amount) extended_amount

from final_cte

group by
 order_date,
fulfill_line_number,
fulfill_org_id,
fulfill_line_id,
ordered_qty_ea,
ship_to_party_site_id,
source_line_number,
line_status,  
ea_price,
customer_po_number,
open_flag,
order_number,
ordered_date,
source_order_number,
source_revision_number,
status_code,
organization_code,
inventory_item_id,
item_number,
description,
party_site_id,
site_use_type,
address_1,
address_2,
city,
country,
postal_code,
state,
cust_acct_id,
account_name,
request_ship_date
)
--56061
select md5(concat(order_date,
fulfill_line_number,
fulfill_org_id,
fulfill_line_id,
ordered_qty_ea,
ship_to_party_site_id,
source_line_number,
line_status,  
ea_price,
customer_po_number,
open_flag,
order_number,
ordered_date,
source_order_number,
source_revision_number,
status_code,
organization_code,
inventory_item_id,
item_number,
description,
party_site_id,
site_use_type,
address_1,
address_2,
city,
country,
postal_code,
state,
cust_acct_id,
account_name,
request_ship_date)) as s_key,
order_date,
fulfill_line_number,
fulfill_org_id,
fulfill_line_id,
ordered_qty_ea,
ship_to_party_site_id,
source_line_number,
line_status,  
ea_price,
customer_po_number,
open_flag,
order_number,
ordered_date,
source_order_number,
source_revision_number,
status_code,
organization_code,
inventory_item_id,
item_number,
description,
party_site_id,
site_use_type,
address_1,
address_2,
city,
country,
postal_code,
state,
cust_acct_id,
account_name,
request_ship_date,
extended_amount,
current_datetime() as load_date_time
from aggregrate_cte 
