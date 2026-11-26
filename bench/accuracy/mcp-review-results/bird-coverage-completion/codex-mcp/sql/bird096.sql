SELECT COUNT(DISTINCT name) AS card_count
FROM main.cards
WHERE isTextless = 0
  AND CAST(frameVersion AS INTEGER) >= 2015
LIMIT 1
