SELECT COUNT(*) AS card_count
FROM cards
WHERE borderColor = 'black'
  AND CAST(power AS REAL) >= 10
  AND availability LIKE '%mtgo%'
