WITH deliveries AS (
    SELECT b.match_id,
           b.innings_no,
           MIN(b.striker, b.non_striker) AS batter_a,
           MAX(b.striker, b.non_striker) AS batter_b,
           b.striker,
           s.runs_scored
    FROM ball_by_ball AS b
    LEFT JOIN batsman_scored AS s
      USING (match_id, innings_no, over_id, ball_id)
), pair_scores AS (
    SELECT match_id,
           batter_a,
           batter_b,
           SUM(CASE WHEN striker = batter_a THEN COALESCE(runs_scored, 0) ELSE 0 END) AS score_a,
           SUM(CASE WHEN striker = batter_b THEN COALESCE(runs_scored, 0) ELSE 0 END) AS score_b
    FROM deliveries
    GROUP BY match_id, innings_no, batter_a, batter_b
), ranked AS (
    SELECT *,
           score_a + score_b AS partnership_score,
           DENSE_RANK() OVER (
               PARTITION BY match_id
               ORDER BY score_a + score_b DESC
           ) AS partnership_rank
    FROM pair_scores
)
SELECT match_id,
       CASE WHEN score_a > score_b THEN batter_a ELSE batter_b END AS player_1,
       CASE WHEN score_a > score_b THEN batter_b ELSE batter_a END AS player_2,
       MAX(score_a, score_b) AS player_1_score,
       MIN(score_a, score_b) AS player_2_score,
       partnership_score
FROM ranked
WHERE partnership_rank = 1
ORDER BY match_id, player_1, player_2;
