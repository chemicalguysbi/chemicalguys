{{
    config(
        materialized = 'table',
		tags = 'cg_dependent_demand'	
    )
}}

with v_dependent_demand_col as (
select *,current_date as load_datetime from 
`cg-gbq-p.oracle_nets.v_Dependent_Demand`)
select * from v_dependent_demand_col