
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    SELECT coverage_fline_details_doc_system_ref_id,			
doc_references_drop_ship_doc_system_ref_id,			
document_references_doc_id FROM {{source('oracle_fusion_fscm','fulfill_line')}} 


)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
