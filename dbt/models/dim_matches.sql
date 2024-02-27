{{config(materialized='table')}}

select match_id, event, venue, city, gender,
       date as dates_played,
       winner, method, player_of_match,
       outcome as other_result, umpire
from {{source("core","match_info")}}