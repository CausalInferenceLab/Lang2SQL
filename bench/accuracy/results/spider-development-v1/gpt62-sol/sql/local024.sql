WITH appearances AS (
    SELECT DISTINCT player_id, match_id
    FROM player_match
), match_runs AS (
    SELECT b.striker AS player_id,
           b.match_id,
           SUM(s.runs_scored) AS runs
    FROM ball_by_ball AS b
    JOIN batsman_scored AS s
      ON s.match_id = b.match_id
     AND s.innings_no = b.innings_no
     AND s.over_id = b.over_id
     AND s.ball_id = b.ball_id
    GROUP BY b.striker, b.match_id
), player_averages AS (
    SELECT a.player_id,
           AVG(COALESCE(r.runs, 0)) AS avg_runs_per_match
    FROM appearances AS a
    LEFT JOIN match_runs AS r
      ON r.player_id = a.player_id
     AND r.match_id = a.match_id
    GROUP BY a.player_id
)
SELECT p.country_name,
       AVG(pa.avg_runs_per_match) AS country_batting_average
FROM player_averages AS pa
JOIN player AS p ON p.player_id = pa.player_id
WHERE p.country_name IS NOT NULL
GROUP BY p.country_name
ORDER BY country_batting_average DESC, p.country_name ASC
LIMIT 5;
