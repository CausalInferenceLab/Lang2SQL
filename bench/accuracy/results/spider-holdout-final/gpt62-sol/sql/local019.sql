SELECT winner.name AS winner_name, loser.name AS loser_name
FROM Matches AS m
JOIN Belts AS b ON b.id = m.title_id
JOIN Wrestlers AS winner ON winner.id = m.winner_id
JOIN Wrestlers AS loser ON loser.id = m.loser_id
WHERE b.name LIKE '%NXT%'
  AND m.title_change = 0
  AND instr(m.duration, ':') > 0
ORDER BY 60 * CAST(substr(m.duration, 1, instr(m.duration, ':') - 1) AS INTEGER)
         + CAST(substr(m.duration, instr(m.duration, ':') + 1) AS INTEGER),
         m.id
LIMIT 1;
