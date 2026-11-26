SELECT COUNT(DISTINCT c.uuid) AS printing_count
FROM main.cards AS c
JOIN main.rulings AS r ON r.uuid = c.uuid
WHERE (c.power IS NULL OR c.power LIKE '%*%')
  AND r.text LIKE '%triggered ability%'
LIMIT 1
