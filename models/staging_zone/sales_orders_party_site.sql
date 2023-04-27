{{
    config(
        materialized = 'table',
		tags = 'sales_orders_party_site'	
    )
}}
with sales_orders_party_site as (
SELECT *
FROM
  cg-gbq-p.oracle_fusion_fscm_partiesanalytics.party_site
)

select *, current_datetime() as load_date_time from sales_orders_party_site
