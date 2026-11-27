WITH qualifying_batters AS (
    SELECT b.striker AS player_id
    FROM ball_by_ball AS b
    JOIN batsman_scored AS s
      ON s.match_id = b.match_id
     AND s.innings_no = b.innings_no
     AND s.over_id = b.over_id
     AND s.ball_id = b.ball_id
    JOIN "match" AS m
      ON m.match_id = b.match_id
    WHERE (b.team_batting = m.team_1 AND m.match_winner = m.team_2)
       OR (b.team_batting = m.team_2 AND m.match_winner = m.team_1)
    GROUP BY b.match_id, b.striker
    HAVING SUM(s.runs_scored) >= 100
)
SELECT DISTINCT p.player_name
FROM qualifying_batters AS q
JOIN player AS p ON p.player_id = q.player_id;
