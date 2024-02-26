{{config(materialized='table')}}

select registry as player_id,
       player_name,
       array_agg(distinct Team) as Teams_played,
       array_agg(distinct match_id) as Matches_played
       from {{source("core",'player_info')}}
       group by 1,2
