SELECT COUNT(*) AS hero_count
FROM superhero AS s
JOIN alignment AS a ON a.id = s.alignment_id
WHERE a.alignment = 'Neutral'
  AND TRIM(s.full_name) NOT IN ('', '-')
LIMIT 1
