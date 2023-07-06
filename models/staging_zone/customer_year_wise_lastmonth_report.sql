 {{
    config(
        materialized = 'table',
		tags = 'customer_year_wise_lastmonth_report'	
    )
}}

with previous_month_customer_data as (
select *, current_datetime as load_datetime  from {{ ref('customer_year_wise_report') }}
)
select * from previous_month_customer_data