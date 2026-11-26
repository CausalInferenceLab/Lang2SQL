WITH qualifying_cards AS (
    SELECT c.scryfallOracleId
    FROM main.cards AS c
    GROUP BY c.scryfallOracleId
    HAVING MAX(c.hasContentWarning) = 1
       AND MAX(instr(',' || c.availability || ',', ',paper,')) > 0
       AND MAX(instr(',' || c.availability || ',', ',mtgo,')) = 0
)
SELECT COUNT(*) AS card_count
FROM qualifying_cards
LIMIT 1
