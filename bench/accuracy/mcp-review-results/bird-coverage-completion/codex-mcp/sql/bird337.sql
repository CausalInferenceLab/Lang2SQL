SELECT ROUND(
    100.0 * COUNT(CASE WHEN p.publisher_name = 'Marvel Comics' THEN 1 END)
    / NULLIF(COUNT(*), 0),
    2
) AS marvel_percentage
FROM superhero AS s
LEFT JOIN publisher AS p ON p.id = s.publisher_id
WHERE s.height_cm BETWEEN 150 AND 180
LIMIT 1000
