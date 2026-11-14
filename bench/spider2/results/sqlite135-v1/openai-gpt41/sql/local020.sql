SELECT
  p.player_name,
  (CAST(SUM(b.bowler_runs) AS FLOAT) / COUNT(w.wicket_id)) AS bowling_average
FROM
  wicket_taken w
JOIN
  ball_by_ball b ON w.match_id = b.match_id AND w.over_id = b.over_id AND w.ball_id = b.ball_id
JOIN
  player p ON b.bowler = p.player_id
GROUP BY
  p.player_id
HAVING
  COUNT(w.wicket_id) > 0
ORDER BY
  bowling_average ASC
LIMIT 1;
