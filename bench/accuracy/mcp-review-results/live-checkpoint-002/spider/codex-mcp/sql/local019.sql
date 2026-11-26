WITH eligible_matches AS (
    SELECT m.winner_id, m.loser_id, m.duration
    FROM main.Matches AS m
    JOIN main.Belts AS b ON b.id = m.title_id
    WHERE b.name LIKE '%NXT%'
      AND b.name NOT LIKE '%title change%'
      AND m.title_change = 0
      AND m.duration <> ''
)
SELECT DISTINCT w.name AS winner, l.name AS loser
FROM eligible_matches AS e
JOIN main.Wrestlers AS w ON w.id = e.winner_id
JOIN main.Wrestlers AS l ON l.id = e.loser_id
WHERE e.duration = (SELECT MIN(duration) FROM eligible_matches)
LIMIT 1000
