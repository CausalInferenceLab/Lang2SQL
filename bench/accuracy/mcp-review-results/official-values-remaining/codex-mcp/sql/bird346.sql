SELECT COUNT(*) AS superhero_count
FROM main.superhero AS s
JOIN main.alignment AS a ON a.id = s.alignment_id
WHERE a.alignment NOT IN ('Good', 'Bad')
LIMIT 1
