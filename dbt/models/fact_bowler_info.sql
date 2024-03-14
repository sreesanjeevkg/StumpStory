{{ config(materialized="table") }}

with
    bowler_details as (
        select
            match_id,
            innings,
            bowler as player,
            count(bowler) as balls_bowled,
            cast(
                floor(
                    sum(
                        case
                            when extras_type = 'NOBALL' or extras_type = 'WIDE'
                            then 0
                            else 1
                        end
                    )
                    / 6
                ) as int
            ) as overs,
            mod(
                sum(
                    case
                        when extras_type = 'NOBALL' or extras_type = 'WIDE'
                        then 0
                        else 1
                    end
                ),
                6
            ) as balls,
            sum(
                case
                    when
                        wicket_type
                        in ('caught', 'bowled', 'stumped', 'lbw', 'caught and bowled')
                    then 1
                    else 0
                end
            ) as wickets,
            sum(runs_off_bat) + sum(
                case
                    when extras_type = 'NOBALL' or extras_type = 'WIDE'
                    then extras
                    else 0
                end
            ) as runs_conceded,
            sum(case when extras_type = 'WIDE' then 1 else 0 end) as wd,
            sum(case when extras_type = 'NOBALL' then 1 else 0 end) as nb
        from {{ ref("fact_balls") }}
        group by match_id, innings, bowler
    ),
    players as (
        select match_id, player_id, player_name, team from {{ ref("dim_players") }}
    ),
    matches as (
        select match_id, team, event, extract(year from dates_played) as match_year
        from {{ ref("dim_matches") }}
    ),

select *
from bowler_details
order by match_id, innings
