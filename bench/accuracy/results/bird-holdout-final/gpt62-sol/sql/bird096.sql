SELECT COUNT(DISTINCT name) AS card_count
FROM cards
WHERE isTextless = 0
  AND CAST(frameVersion AS INTEGER) >= 2015;
