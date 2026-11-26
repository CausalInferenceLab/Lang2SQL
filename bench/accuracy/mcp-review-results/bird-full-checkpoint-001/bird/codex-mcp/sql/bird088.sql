WITH eligible_cards AS (
    SELECT DISTINCT name
    FROM main.cards
    WHERE originalType = 'Creature - Elf'
),
distinct_rulings AS (
    SELECT DISTINCT c.name, r.date, r.text
    FROM eligible_cards AS e
    JOIN main.cards AS c ON c.name = e.name
    JOIN main.rulings AS r ON r.uuid = c.uuid
),
ruling_counts AS (
    SELECT e.name, COUNT(r.name) AS ruling_count
    FROM eligible_cards AS e
    LEFT JOIN distinct_rulings AS r ON r.name = e.name
    GROUP BY e.name
)
SELECT name, ruling_count
FROM ruling_counts
WHERE ruling_count = (SELECT MAX(ruling_count) FROM ruling_counts)
ORDER BY name
LIMIT 1000
