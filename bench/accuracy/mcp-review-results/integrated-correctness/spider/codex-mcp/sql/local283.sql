WITH team_matches AS (
    SELECT
        season,
        league_id,
        country_id,
        home_team_api_id AS team_api_id,
        home_team_goal AS goals_for,
        away_team_goal AS goals_against
    FROM main."Match"
    UNION ALL
    SELECT
        season,
        league_id,
        country_id,
        away_team_api_id,
        away_team_goal,
        home_team_goal
    FROM main."Match"
),
season_points AS (
    SELECT
        season,
        league_id,
        country_id,
        team_api_id,
        SUM(CASE
            WHEN goals_for > goals_against THEN 3
            WHEN goals_for = goals_against THEN 1
            ELSE 0
        END) AS total_points
    FROM team_matches
    GROUP BY season, league_id, country_id, team_api_id
),
ranked_teams AS (
    SELECT
        season,
        league_id,
        country_id,
        team_api_id,
        total_points,
        RANK() OVER (
            PARTITION BY season
            ORDER BY total_points DESC
        ) AS points_rank
    FROM season_points
)
SELECT
    r.season,
    t.team_long_name AS team_name,
    l.name AS league,
    c.name AS country,
    r.total_points
FROM ranked_teams AS r
JOIN main.Team AS t ON t.team_api_id = r.team_api_id
JOIN main.League AS l ON l.id = r.league_id
JOIN main.Country AS c ON c.id = r.country_id
WHERE r.points_rank = 1
ORDER BY r.season, team_name, league, country
LIMIT 1000
