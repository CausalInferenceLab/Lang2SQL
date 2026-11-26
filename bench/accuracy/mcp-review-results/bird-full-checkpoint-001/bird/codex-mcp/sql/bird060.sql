SELECT 100.0 * COUNT(CASE
           WHEN c.cardKingdomId IS NOT NULL AND c.cardKingdomFoilId IS NOT NULL
           THEN 1
       END) / NULLIF(COUNT(*), 0) AS percentage_with_both_ids
FROM cards AS c
WHERE c.setCode IN (SELECT s.code FROM sets AS s WHERE s.name = 'Coldsnap')
LIMIT 1
