WITH matching_sets AS (
    SELECT s.code, s.releaseDate
    FROM main.sets AS s
    WHERE s.code IN (
        SELECT c.setCode
        FROM main.cards AS c
        WHERE c.name = 'Ancestor''s Chosen'
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
  AND CAST(CAST(c.power AS INTEGER) AS TEXT) = c.power
  AND CAST(CAST(c.toughness AS INTEGER) AS TEXT) = c.toughness
  AND CAST(c.power AS INTEGER) > CAST(c.toughness AS INTEGER)
LIMIT 1
