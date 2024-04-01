{{ config(materialized="table") }}

with
    bowler_details as (
        select
            match_id,
            innings,
            bowler as player,
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
                ) as string
            ) as overs,
            cast(
                mod(
                    sum(
                        case
                            when extras_type = 'NOBALL' or extras_type = 'WIDE'
                            then 0
                            else 1
                        end
                    ),
                    6
                ) as string
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
    )
select
    match_id,
    innings,
    player as bowler,
    concat(overs, '.', balls) as overs,
    wickets,
    runs_conceded as runs,
    wd as wide,
    nb as noball
from bowler_details
