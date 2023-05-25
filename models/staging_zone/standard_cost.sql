{{
    config(
        materialized='table',
        tags = 'standard_cost'
    )
}}

select * except(rno) from (select organization_code,organization_id,b.inventory_item_id,a.item_number,
    cst_org_name,
    effective_start_date,
    effective_end_date,
    current_datetime() as load_datetime,
    current_cost,row_number() over(partition by a.item_number,cast(a.organization_id as string) order by a.effective_start_date desc) rno
from `cg-gbq-p.oracle_nets.standard_cost` a
left join cg-gbq-p.consumption_zone.cg_product_dimension b
on a.item_number = b.item_number)
where rno = 1
order by current_cost

