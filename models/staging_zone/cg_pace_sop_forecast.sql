{{
    config(
        materialized = 'table',
		tags = 'cg_pace_sop_forecast'	
    )
}}

with cg_pace_sop_forecast_col as (select main_class_category,main_class_sub_category,cast(amount as float64) as amount,PARSE_DATE('%m/%d/%Y',  start_date_of_the_month) as main_date
  from `cg-gbq-p.staging_zone.pace_sop_forecast`
)

select *,  current_datetime() as load_datetime from cg_pace_sop_forecast_col