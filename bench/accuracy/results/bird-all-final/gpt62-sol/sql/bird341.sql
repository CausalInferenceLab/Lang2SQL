SELECT ROUND(100.0 * COUNT(CASE WHEN p.publisher_name = 'Marvel Comics' THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS percentage
FROM superhero AS s
JOIN gender AS g ON g.id = s.gender_id
LEFT JOIN publisher AS p ON p.id = s.publisher_id
WHERE g.gender = 'Female';
