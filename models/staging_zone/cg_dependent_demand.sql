{{
    config(
        materialized = 'table',
		tags = 'cg_dependent_demand'	
    )
}}

with v_dependent_demand_col as (
select  distinct `KEY`, ITEM_NUMBER, ORGANIZATION_CODE, WEEK_NUM, YEAR, SO_DEMAND, FCT_DEMAND, ACT_DEMAND, COMPONENT, round(REQ_QTY,2) as REQ_QTY , round(DEPENDENT_DEMAND,2) as DEPENDENT_DEMAND
 from`cg-gbq-p.oracle_nets.v_Dependent_Demand`)
select *, current_datetime as load_datetime from v_dependent_demand_col