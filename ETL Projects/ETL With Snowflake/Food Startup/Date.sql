use role sysadmin;
use database sandbox;
use warehouse adho_wh;
use schema consumption_sch;

create or replace table consumption_sch.date_dim (
    date_dim_hk number primary key,
    calendar_date date unique,
    year number,
    quarter number,
    month number,
    week number,
    day_of_year number,
    day_of_week number,
    day_of_month number,
    day_name string
) 
comment = 'date dimension table created using the min date of order'
;

insert into consumption_sch.date_dim 
with recursive my_date_dim_cte as (
    select 
        current_date() as today,
        year(today) as year,
        quarter(today) as quarter,
        month(today) as month,
        week(today) as week,
        dayofyear(today) as day_of_year,
        dayofweek(today) as day_of_week,
        dayofmonth(today) as day_of_month,
        dayname(today) as day_name
    union all
        select 
            dateadd('day',-1, today) as today_r,
            year(today_r) as year,
            quarter(today_r) as quarter,
            month(today_r) as month,
            week(today_r) as week,
            dayofyear(today_r) as day_of_year,
            dayofweek(today_r) as day_of_week,
            dayofmonth(today_r) as day_of_month,
            dayname(today_r) as day_name
    from 
        my_date_dim_cte
    where 
        today_r>(select date(min(order_date)) from clean_sch.orders)
)
select 

    hash(sha1_hex(today)) as date_dim_hk,
    today,
    year,
    quarter,
    month,
    week,
    day_of_year,
    day_of_week,
    day_of_month,
    day_name
    
from my_date_dim_cte;

select  * from consumption_sch.date_dim;