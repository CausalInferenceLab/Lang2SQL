SELECT COUNT(*) AS card_count
FROM cards
WHERE borderColor = 'white'
  AND power NOT LIKE '%*%'
  AND CAST(power AS REAL) < 3;
