WITH batting_totals AS (
    SELECT m.season_id, b.striker AS player_id, SUM(s.runs_scored) AS total_runs
    FROM batsman_scored AS s
    JOIN ball_by_ball AS b USING (match_id, innings_no, over_id, ball_id)
    JOIN "match" AS m ON m.match_id = b.match_id
    GROUP BY m.season_id, b.striker
), bowling_totals AS (
    SELECT m.season_id, b.bowler AS player_id, COUNT(*) AS total_wickets
    FROM wicket_taken AS w
    JOIN ball_by_ball AS b USING (match_id, innings_no, over_id, ball_id)
    JOIN "match" AS m ON m.match_id = b.match_id
    WHERE LOWER(TRIM(w.kind_out)) NOT IN ('run out', 'hit wicket', 'retired hurt')
    GROUP BY m.season_id, b.bowler
), ranked_batsmen AS (
    SELECT season_id, player_id, total_runs,
           ROW_NUMBER() OVER (
               PARTITION BY season_id ORDER BY total_runs DESC, player_id ASC
           ) AS position
    FROM batting_totals
), ranked_bowlers AS (
    SELECT season_id, player_id, total_wickets,
           ROW_NUMBER() OVER (
               PARTITION BY season_id ORDER BY total_wickets DESC, player_id ASC
           ) AS position
    FROM bowling_totals
)
SELECT bat.season_id,
       batter.player_name AS batsman_name,
       bat.total_runs,
       bowler.player_name AS bowler_name,
       bowl.total_wickets
FROM ranked_batsmen AS bat
JOIN ranked_bowlers AS bowl
  ON bowl.season_id = bat.season_id AND bowl.position = bat.position
JOIN player AS batter ON batter.player_id = bat.player_id
JOIN player AS bowler ON bowler.player_id = bowl.player_id
WHERE bat.position <= 3
ORDER BY bat.season_id ASC, bat.position ASC;
