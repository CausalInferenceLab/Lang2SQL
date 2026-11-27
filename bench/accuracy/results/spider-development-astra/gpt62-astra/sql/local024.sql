WITH match_runs AS (
    SELECT b.match_id, b.striker AS player_id, SUM(s.runs_scored) AS runs
    FROM ball_by_ball AS b
    JOIN batsman_scored AS s
      USING (match_id, innings_no, over_id, ball_id)
    GROUP BY b.match_id, b.striker
),
player_averages AS (
    SELECT pm.player_id, AVG(COALESCE(mr.runs, 0)) AS avg_runs_per_match
    FROM player_match AS pm
    LEFT JOIN match_runs AS mr
      USING (match_id, player_id)
    GROUP BY pm.player_id
)
SELECT p.country_name, AVG(pa.avg_runs_per_match) AS country_batting_average
FROM player_averages AS pa
JOIN player AS p USING (player_id)
WHERE p.country_name IS NOT NULL
GROUP BY p.country_name
ORDER BY country_batting_average DESC, p.country_name ASC
LIMIT 5
