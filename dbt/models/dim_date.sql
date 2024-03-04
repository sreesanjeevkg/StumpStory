{{ config(materialized="table") }}
with recursive cte as (
        select
            date '2002-01-01' as cal_date,
            extract(year from date '2002-01-01') as cal_year,
            extract(dayofyear from date '2002-01-01') as cal_year_day,
            extract(quarter from date '2002-01-01') as cal_quarter,
            extract(month from date '2002-01-01') as cal_month,
            format_date('%B', date '2002-01-01') as cal_month_name,
            extract(day from date '2002-01-01') as cal_month_day,
            extract(week from date '2002-01-01') as cal_week,
            extract(dayofweek from date '2002-01-01') as cal_week_day,
            format_date('%A', date '2002-01-01') as cal_day_name
        union all
        select
            date_add(cal_date, interval 1 day) as cal_date,
            extract(year from date_add(cal_date, interval 1 day)) as cal_year,
            extract(dayofyear from date_add(cal_date, interval 1 day)) as cal_year_day,
            extract(quarter from date_add(cal_date, interval 1 day)) as cal_quarter,
            extract(month from date_add(cal_date, interval 1 day)) as cal_month,
            format_date('%B', date_add(cal_date, interval 1 day)) as cal_month_name,
            extract(day from date_add(cal_date, interval 1 day)) as cal_month_day,
            extract(week from date_add(cal_date, interval 1 day)) as cal_week,
            extract(dayofweek from date_add(cal_date, interval 1 day)) as cal_week_day,
            format_date('%A', date_add(cal_date, interval 1 day)) as cal_day_name
        from cte
        where cal_date < date '2030-12-31'
    )
select *, row_number() over (order by cal_date asc) as id
from cte
order by id asc

