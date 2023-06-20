{{
    config(
        materialized = 'table',
		tags = 'cg_demand_forcast'	
    )
}}

with demand_forcast_col as (
select *,
  current_datetime() as load_datetime
  from `cg-gbq-p.oracle_nets.demand_forecast`
)

select * from demand_forcast_col