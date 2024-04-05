{{ config(materialized="table") }}

with
    unnested_rows as (
        select
            match_id,
            event,
            venue,
            city,
            gender,
            winner,
            method,
            regexp_replace(team, r"[\[\]']", "") as team,
            regexp_replace(player, r"[\[\]']", "") as pom,
            outcome as other_result,
            umpire,
            regexp_replace(dates, r"[\[\]]", "") as dates_played,
            player_of_match
        from {{ source("core", "match") }}
        left join unnest(split(date, ", ")) as dates
        left join unnest(split(player_of_match, ", ")) as player
        left join unnest(split(teams, ", ")) as team
    )
select
    match_id,
    team,
    parse_date('%Y/%m/%d', regexp_replace(dates_played, "'", '')) as dates_played,
    event,
    venue,
    city,
    gender,
    winner,
    method,
    coalesce(pom, player_of_match, null) as player_of_match,
    other_result,
    umpire
from unnested_rows
