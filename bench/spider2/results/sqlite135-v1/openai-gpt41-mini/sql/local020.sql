WITH bowler_stats AS (
    SELECT
        ball_by_ball.bowler AS bowler_id,
        COUNT(wicket_taken.player_out) AS wickets,
        SUM(batsman_scored.runs_scored) AS runs_conceded
    FROM ball_by_ball
    LEFT JOIN wicket_taken ON ball_by_ball.match_id = wicket_taken.match_id
                             AND ball_by_ball.over_id = wicket_taken.over_id
                             AND ball_by_ball.ball_id = wicket_taken.ball_id
    JOIN batsman_scored ON ball_by_ball.match_id = batsman_scored.match_id
                        AND ball_by_ball.over_id = batsman_scored.over_id
                        AND ball_by_ball.ball_id = batsman_scored.ball_id
    WHERE ball_by_ball.bowler IS NOT NULL
    GROUP BY ball_by_ball.bowler
)
SELECT player.player_name, (runs_conceded * 1.0 / wickets) AS bowling_average
FROM bowler_stats
JOIN player ON bowler_stats.bowler_id = player.player_id
WHERE wickets > 0
ORDER BY bowling_average ASC
LIMIT 1;
