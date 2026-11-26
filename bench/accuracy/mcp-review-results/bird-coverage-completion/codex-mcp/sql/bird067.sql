SELECT COUNT(DISTINCT r.text) AS ruling_count
FROM main.rulings AS r
JOIN main.cards AS c ON c.uuid = r.uuid
WHERE c.name = 'Mishra''s Factory'
GROUP BY r.date
ORDER BY r.date DESC
LIMIT 1;
