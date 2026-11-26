WITH matching_sets AS (
    SELECT s.code, s.releaseDate
    FROM main.sets AS s
    WHERE s.code IN (
        SELECT setCode
        FROM main.cards
        WHERE name = 'Ancestor''s Chosen'
    )
)
SELECT COUNT(*) AS creature_count
FROM main.cards AS c
WHERE c.setCode IN (
    SELECT code
    FROM matching_sets
    WHERE releaseDate = (SELECT MAX(releaseDate) FROM matching_sets)
)
  AND instr(',' || c.types || ',', ',Creature,') > 0
  AND instr(c.power, '*') = 0
  AND instr(c.toughness, '*') = 0
  AND CAST(c.power AS REAL) > CAST(c.toughness AS REAL)
LIMIT 1
