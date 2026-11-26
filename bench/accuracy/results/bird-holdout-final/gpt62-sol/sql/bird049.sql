SELECT id
FROM cards
WHERE borderColor = 'borderless'
  AND (cardKingdomId IS NULL OR cardKingdomFoilId IS NULL);
