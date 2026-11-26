WITH rating_periods AS (
    SELECT
        team_api_id,
        buildUpPlaySpeedClass,
        date AS start_date,
        LEAD(date) OVER (
            PARTITION BY team_api_id
            ORDER BY date, id
        ) AS next_date
    FROM main.Team_Attributes
)
SELECT
    t.team_long_name,
    t.team_short_name,
    COUNT(*) AS home_wins
FROM main."Match" AS m
JOIN main.Team AS t
    ON t.team_api_id = m.home_team_api_id
JOIN rating_periods AS r
    ON r.team_api_id = m.home_team_api_id
   AND m.date >= r.start_date
   AND (m.date < r.next_date OR r.next_date IS NULL)
WHERE m.home_team_goal - m.away_team_goal >= 3
  AND r.buildUpPlaySpeedClass = 'Fast'
GROUP BY t.team_api_id, t.team_long_name, t.team_short_name
ORDER BY home_wins DESC, t.team_long_name, t.team_api_id
LIMIT -1
