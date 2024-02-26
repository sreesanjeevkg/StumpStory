{{
    config(
        materialized='table'
    )
}}

WITH deliveries AS (
    SELECT 
        match_id,
        innings,
        SPLIT(CAST(ball AS string), '.')[OFFSET(0)] AS overs,
        SPLIT(CAST(ball AS string), '.')[OFFSET(1)] AS ball,
        striker,
        non_striker,
        bowler,
        runs_off_bat,
        extras,
        wides,
        noballs,
        byes,
        legbyes,
        penalty,
        wicket_type,
        player_dismissed,
        other_wicket_type,
        other_player_dismissed
    FROM 
        {{ source('core', 'ball_by_ball') }}
),
players AS (
    SELECT 
        player_name, 
        player_id
    FROM 
        {{ ref("dim_players") }}
),
matches AS (
    SELECT 
        match_id 
    FROM 
        {{ ref("dim_matches") }}
) 
SELECT 
    matches.match_id,
    innings,
    cast overs as integer,
    cast ball as integer,
    b1.player_id AS striker,
    b2.player_id AS non_striker,
    b3.player_id AS bowler,
    runs_off_bat,
    extras,
    CASE 
        WHEN wides IS NOT NULL THEN 'WIDE' 
        WHEN noballs IS NOT NULL THEN 'NOBALL' 
        WHEN byes IS NOT NULL THEN 'BYE' 
        WHEN legbyes IS NOT NULL THEN 'LEGBYES' 
        WHEN penalty IS NOT NULL THEN 'PENALTY' 
    END AS extras_type,
    CASE 
        WHEN wicket_type is null THEN 'N' 
        ELSE 'Y' 
    END AS wicket_flg,
    wicket_type
FROM 
    deliveries
INNER JOIN 
    matches ON CAST(deliveries.match_id AS string) = CAST(matches.match_id AS string)
INNER JOIN 
    players b1 ON deliveries.striker = b1.player_name
INNER JOIN 
    players b2 ON deliveries.non_striker = b2.player_name
INNER JOIN 
    players b3 ON deliveries.bowler = b3.player_name