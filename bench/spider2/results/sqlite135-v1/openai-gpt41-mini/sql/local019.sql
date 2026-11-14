WITH nxt_matches AS (
  SELECT m.*, w1.name AS wrestler1, w2.name AS wrestler2
  FROM Matches m
  JOIN Wrestlers w1 ON m.winner_id = w1.id
  JOIN Wrestlers w2 ON m.loser_id = w2.id
  WHERE m.title_id IN (SELECT id FROM Belts WHERE name LIKE '%NXT%')
    AND m.title_change = 0
)
SELECT wrestler1, wrestler2
FROM nxt_matches
ORDER BY duration ASC
LIMIT 1;
