SELECT COUNT(*) AS card_count
FROM main.cards
WHERE borderColor = 'white'
  AND power NOT LIKE '%*%'
  AND CAST(power AS REAL) < 3
LIMIT 1
