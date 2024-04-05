{{ config(materialized="table") }}

select registry as player_id, player_name, team, match_id
from {{ source("core", "player") }}
group by 1, 2, 3, 4
