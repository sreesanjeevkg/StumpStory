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
        select match_id, team, event, extract(year from dates_played) as match_year
        from {{ ref("dim_matches") }}
    ),
    score_data as (
        select
            matches.match_id,
            match_year,
            matches.event,
            p1.team as team_for,
            p2.team as team_against,
            bat_details.player as batsman,
            innings,
            runs,
            balls_faced,
            `0s`,
            `4s`,
            `6s`,
            round(
                {{ dbt_utils.safe_divide("runs", "balls_faced") }} * 100, 2
            ) as strikerate
        from matches
        inner join
            players p1 on p1.match_id = matches.match_id and p1.team = matches.team
        inner join
            players p2 on p2.match_id = matches.match_id and p2.team <> matches.team
        inner join
            bat_details
            on cast(bat_details.match_id as string) = cast(matches.match_id as string)
            and cast(p1.player_name as string) = cast(bat_details.player as string)
        group by
            match_id,
            match_year,
            event,
            team_for,
            team_against,
            batsman,
            innings,
            runs,
            balls_faced,
            `0s`,
            `4s`,
            `6s`,
            strikerate
        order by match_id, bat_details.innings
    )
select * from score_data
