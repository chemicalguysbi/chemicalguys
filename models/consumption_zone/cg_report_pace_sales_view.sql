{{
       config(
             materialized='view',
             tags = 'cg_report_pace_sales_view'
         )
   }}
select *
from cg-gbq-p.consumption_zone.cg_report_pace_sales