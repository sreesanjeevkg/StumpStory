{{ config(materialized="table") }}

with
    bat_details as (
        select
            match_id,
            innings,
            striker as player,
            sum(runs_off_bat) as runs,
            sum(case when extras_type = 'WIDE' then 0 else 1 end) as balls_faced,
            sum(case when runs_off_bat = 0 then 1 else 0 end)
            - sum(case when extras_type = 'WIDE' then 1 else 0 end) as `0s`,
            sum(case when runs_off_bat = 4 then 1 else 0 end) as `4s`,
            sum(case when runs_off_bat = 6 then 1 else 0 end) as `6s`,
        from {{ ref("fact_balls") }}
        group by match_id, innings, striker
    ),
    players as (
        select match_id, player_id, player_name, team from {{ ref("dim_players") }}
    ),
    matches as (
        select match_id, team, event, dates_played as match_date
        from {{ ref("dim_matches") }}
    ),
    player_dismissed_flg as (
        select match_id, innings, array_agg(player_dismissed) as dismissed_players
        from {{ ref("fact_balls") }}
        where player_dismissed is not null
        group by match_id, innings
    ),
    score_data as (
        select
            matches.match_id,
            match_date,
            matches.event,
            p1.team as team_for,
            p2.team as team_against,
            bat_details.player as batsman,
            bat_details.innings as innings_number,
            runs,
            balls_faced,
            `0s`,
            `4s`,
            `6s`,
            round(
                {{ dbt_utils.safe_divide("runs", "balls_faced") }} * 100, 2
            ) as strikerate,
            case
                when bat_details.player in unnest(dismissed_players) then 'Y' else 'N'
            end as out
        from matches
        inner join
            players p1 on p1.match_id = matches.match_id and p1.team = matches.team
        inner join
            players p2 on p2.match_id = matches.match_id and p2.team <> matches.team
        inner join
            bat_details
            on cast(bat_details.match_id as string) = cast(matches.match_id as string)
            and cast(p1.player_name as string) = cast(bat_details.player as string)
        inner join
            player_dismissed_flg pdf
            on cast(pdf.match_id as string) = cast(bat_details.match_id as string)
            and pdf.innings = bat_details.innings
        group by
            match_id,
            match_date,
            event,
            team_for,
            team_against,
            batsman,
            innings_number,
            runs,
            balls_faced,
            `0s`,
            `4s`,
            `6s`,
            strikerate,
            out
    )
select *
from score_data
