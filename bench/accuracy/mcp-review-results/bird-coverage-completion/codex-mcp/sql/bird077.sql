WITH flash_cards AS (
    SELECT c.name, c.convertedManaCost
    FROM main.cards AS c
    WHERE c.setCode IN (
        SELECT s.code
        FROM main.sets AS s
        WHERE s.name = 'Dark Ascension'
    )
      AND instr(',' || c.keywords || ',', ',Flash,') > 0
)
SELECT DISTINCT name
FROM flash_cards
WHERE convertedManaCost = (SELECT MAX(convertedManaCost) FROM flash_cards)
ORDER BY name
LIMIT -1
