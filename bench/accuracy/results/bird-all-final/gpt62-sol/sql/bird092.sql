SELECT cardKingdomFoilId, cardKingdomId
FROM cards
WHERE (',' || types || ',') LIKE '%,Creature,%'
  AND cardKingdomFoilId IS NOT NULL
  AND cardKingdomId IS NOT NULL
ORDER BY cardKingdomFoilId ASC, cardKingdomId ASC
LIMIT 3;
