WITH slots(slot) AS (
    VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11),
           (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22)
), appearances AS (
    SELECT m.id AS match_id,
           CASE s.slot
               WHEN 1  THEN m.home_player_1
               WHEN 2  THEN m.home_player_2
               WHEN 3  THEN m.home_player_3
               WHEN 4  THEN m.home_player_4
               WHEN 5  THEN m.home_player_5
               WHEN 6  THEN m.home_player_6
               WHEN 7  THEN m.home_player_7
               WHEN 8  THEN m.home_player_8
               WHEN 9  THEN m.home_player_9
               WHEN 10 THEN m.home_player_10
               WHEN 11 THEN m.home_player_11
               WHEN 12 THEN m.away_player_1
               WHEN 13 THEN m.away_player_2
               WHEN 14 THEN m.away_player_3
               WHEN 15 THEN m.away_player_4
               WHEN 16 THEN m.away_player_5
               WHEN 17 THEN m.away_player_6
               WHEN 18 THEN m.away_player_7
               WHEN 19 THEN m.away_player_8
               WHEN 20 THEN m.away_player_9
               WHEN 21 THEN m.away_player_10
               WHEN 22 THEN m.away_player_11
           END AS player_api_id,
           CASE
               WHEN s.slot <= 11 AND m.home_team_goal > m.away_team_goal THEN 'Win'
               WHEN s.slot > 11 AND m.away_team_goal > m.home_team_goal THEN 'Win'
               ELSE 'Loss'
           END AS outcome
    FROM "Match" AS m
    CROSS JOIN slots AS s
    WHERE m.home_team_goal <> m.away_team_goal
), ranked AS (
    SELECT player_api_id, outcome,
           DENSE_RANK() OVER (
               PARTITION BY outcome ORDER BY COUNT(DISTINCT match_id) DESC
           ) AS outcome_rank
    FROM appearances
    WHERE player_api_id IS NOT NULL
    GROUP BY player_api_id, outcome
)
SELECT r.outcome, p.player_name
FROM ranked AS r
JOIN Player AS p ON p.player_api_id = r.player_api_id
WHERE r.outcome_rank = 1
ORDER BY r.outcome DESC, p.player_name;
