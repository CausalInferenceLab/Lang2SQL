WITH flash_cards AS (
    SELECT c.name, c.convertedManaCost
    FROM cards AS c
    JOIN sets AS s ON s.code = c.setCode
    WHERE s.name = 'Dark Ascension'
      AND instr(',' || c.keywords || ',', ',Flash,') > 0
)
SELECT DISTINCT name
FROM flash_cards
WHERE convertedManaCost = (SELECT MAX(convertedManaCost) FROM flash_cards)
ORDER BY name;
