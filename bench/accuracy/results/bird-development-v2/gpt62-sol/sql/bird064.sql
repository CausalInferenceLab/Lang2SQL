SELECT COUNT(*) AS card_count
FROM cards
WHERE borderColor = 'black'
  AND availability LIKE '%mtgo%'
  AND CAST(power AS INTEGER) >= 10;
