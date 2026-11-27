SELECT 100.0 * AVG(CASE
           WHEN NULLIF(TRIM(c.cardKingdomId), '') IS NOT NULL
            AND NULLIF(TRIM(c.cardKingdomFoilId), '') IS NOT NULL
           THEN 1 ELSE 0
       END) AS percentage
FROM cards AS c
WHERE EXISTS (
    SELECT 1
    FROM sets AS s
    WHERE s.code = c.setCode
      AND s.name = 'Coldsnap'
);
