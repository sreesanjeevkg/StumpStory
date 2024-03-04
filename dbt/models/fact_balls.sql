{{ config(materialized="table") }}

with
    deliveries as (
        select
            match_id,
            innings,
            split(cast(ball as string), '.')[offset(0)] as overs,
            split(cast(ball as string), '.')[offset(1)] as ball,
            striker,
            non_striker,
            bowler,
            runs_off_bat,
            extras,
            wides,
            noballs,
            byes,
            legbyes,
            penalty,
            wicket_type,
            player_dismissed,
            other_wicket_type,
            other_player_dismissed
        from {{ source("core", "ball_by_ball") }}
    )
select
    match_id,
    striker,
    non_striker,
    bowler,
    innings,
    cast(overs as integer) + 1 as overs,
    cast(ball as integer) as ball,
    runs_off_bat,
    extras,
    case
        when wides is not null
        then 'WIDE'
        when noballs is not null
        then 'NOBALL'
        when byes is not null
        then 'BYE'
        when legbyes is not null
        then 'LEGBYES'
        when penalty is not null
        then 'PENALTY'
    end as extras_type,
    case when wicket_type is null then 'N' else 'Y' end as wicket_flg,
    wicket_type
from deliveries
