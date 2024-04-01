{{ config(materialized="table") }}

select
    batsman,
    event,
    count(batsman) as innings,
    array_agg(distinct team_for) as teams,
    sum(runs) as runs,
    sum(balls_faced) as balls,
    sum(`0s`) as `0s`,
    sum(`4s`) as `4s`,
    sum(`6s`) as `6s`,
    sum(case when out = 'Y' then 1 else 0 end) as outs,
    max(runs) as HS
from {{ ref("fact_batsman_info_by_innings") }}
group by batsman, event