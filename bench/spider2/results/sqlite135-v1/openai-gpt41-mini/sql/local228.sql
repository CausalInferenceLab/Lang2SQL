WITH top_batsmen AS (
  SELECT season_id, batsman_id AS player_id, SUM(runs) AS total_runs,
         ROW_NUMBER() OVER (PARTITION BY season_id ORDER BY SUM(runs) DESC, batsman_id ASC) AS rn
  FROM batsman_scored
  GROUP BY season_id, batsman_id
  HAVING rn <= 3
),
top_bowlers AS (
  SELECT m.season_id, w.player_id, COUNT(*) AS total_wickets,
         ROW_NUMBER() OVER (PARTITION BY m.season_id ORDER BY COUNT(*) DESC, w.player_id ASC) AS rn
  FROM wicket_taken w
  JOIN match m ON w.match_id = m.match_id
  WHERE w.dismissal_type NOT IN ('run out', 'hit wicket', 'retired hurt')
  GROUP BY m.season_id, w.player_id
  HAVING rn <= 3
),
top_batsmen_rn AS (
  SELECT season_id, player_id, total_runs, rn
  FROM (
    SELECT season_id, batsman_id AS player_id, SUM(runs) AS total_runs,
           ROW_NUMBER() OVER (PARTITION BY season_id ORDER BY SUM(runs) DESC, batsman_id ASC) AS rn
    FROM batsman_scored
    GROUP BY season_id, batsman_id
  )
  WHERE rn <= 3
),
top_bowlers_rn AS (
  SELECT season_id, player_id, total_wickets, rn
  FROM (
    SELECT m.season_id, w.player_id, COUNT(*) AS total_wickets,
           ROW_NUMBER() OVER (PARTITION BY m.season_id ORDER BY COUNT(*) DESC, w.player_id ASC) AS rn
    FROM wicket_taken w
    JOIN match m ON w.match_id = m.match_id
    WHERE w.dismissal_type NOT IN ('run out', 'hit wicket', 'retired hurt')
    GROUP BY m.season_id, w.player_id
  )
  WHERE rn <= 3
)

SELECT b.season_id,
       b.player_id AS batsman_id,
       b.total_runs,
       bw.player_id AS bowler_id,
       bw.total_wickets
FROM top_batsmen_rn b
JOIN top_bowlers_rn bw ON b.season_id = bw.season_id AND b.rn = bw.rn
ORDER BY b.season_id, b.rn
;
