{{
    config(
        materialized='table'
    )
}}

with deliveries as (select match_id,
    innings,
    split(ball,'.')[0]::int + 1 as 'over',
    split(ball,'.')[1]::int as ball,
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
 from {{ source('core','ball_by_ball')}})
 players as 
 select * from deliveries