WITH chosen_sets AS (
    SELECT s.code, s.releaseDate
    FROM sets AS s
    WHERE EXISTS (
        SELECT 1
        FROM cards AS a
        WHERE a.setCode = s.code
          AND a.name = 'Ancestor''s Chosen'
    )
)
SELECT COUNT(*) AS creature_count
FROM cards AS c
WHERE c.setCode IN (
    SELECT code
    FROM chosen_sets
    WHERE releaseDate = (SELECT MAX(releaseDate) FROM chosen_sets)
)
  AND (',' || REPLACE(c.types, ' ', '') || ',') LIKE '%,Creature,%'
  AND c.power = CAST(CAST(c.power AS INTEGER) AS TEXT)
  AND c.toughness = CAST(CAST(c.toughness AS INTEGER) AS TEXT)
  AND CAST(c.power AS INTEGER) > CAST(c.toughness AS INTEGER);
