{{config(materialized='table')}}

with unnested_rows as (
    SELECT
    match_id, event, venue, city, gender,
    winner, method,
    REGEXP_REPLACE(player, r"[\[\]']", "") as POM,
    outcome as other_result, umpire,
    REGEXP_REPLACE(dates, r"[\[\]]", "") AS dates_played,
    player_of_match
FROM {{source("core","match_info")}}
LEFT JOIN
  UNNEST(SPLIT(date, ", ")) AS dates
LEFT JOIN
  UNNEST(SPLIT(player_of_match, ", ")) AS player)
select 
    match_id,
    PARSE_DATE('%Y/%m/%d', REGEXP_REPLACE(dates_played, "'", '')) as dates_played,
    event, venue, city, gender,
    winner, method,coalesce(POM, player_of_match, null) as player_of_match,
    other_result, umpire
from unnested_rows