WITH elf_cards AS (
    SELECT DISTINCT scryfallOracleId, name
    FROM main.cards
    WHERE originalType = 'Creature - Elf'
),
distinct_rulings AS (
    SELECT DISTINCT e.scryfallOracleId, r.date, r.text
    FROM elf_cards AS e
    JOIN main.cards AS c ON c.scryfallOracleId = e.scryfallOracleId
    JOIN main.rulings AS r ON r.uuid = c.uuid
),
ruling_counts AS (
    SELECT e.name, COUNT(r.scryfallOracleId) AS ruling_count
    FROM elf_cards AS e
    LEFT JOIN distinct_rulings AS r ON r.scryfallOracleId = e.scryfallOracleId
    GROUP BY e.scryfallOracleId, e.name
)
SELECT name, ruling_count
FROM ruling_counts
WHERE ruling_count = (SELECT MAX(ruling_count) FROM ruling_counts)
ORDER BY name
LIMIT -1
