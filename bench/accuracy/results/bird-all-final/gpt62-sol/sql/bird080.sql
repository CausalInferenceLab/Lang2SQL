SELECT COUNT(*) AS card_count
FROM cards
WHERE duelDeck = 'b'
  AND hasFoil = 0
  AND power = CAST(CAST(power AS INTEGER) AS TEXT)
  AND CAST(power AS INTEGER) < 3;
