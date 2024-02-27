{{config(materialized='table')}}

select registry as player_id,
       player_name,
       Team,
       match_id
       from {{source("core",'player_info')}}
       group by 1,2,3,4
