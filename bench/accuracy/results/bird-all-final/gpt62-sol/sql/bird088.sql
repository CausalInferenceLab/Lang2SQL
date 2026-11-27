WITH elf_cards AS (
    SELECT DISTINCT name
    FROM cards
    WHERE originalType = 'Creature - Elf'
), distinct_rulings AS (
    SELECT DISTINCT e.name, r.date, r.text
    FROM elf_cards AS e
    JOIN cards AS c ON c.name = e.name
    JOIN rulings AS r ON r.uuid = c.uuid
), ruling_counts AS (
    SELECT e.name, COUNT(r.name) AS ruling_count
    FROM elf_cards AS e
    LEFT JOIN distinct_rulings AS r ON r.name = e.name
    GROUP BY e.name
)
SELECT name, ruling_count
FROM ruling_counts
WHERE ruling_count = (SELECT MAX(ruling_count) FROM ruling_counts)
ORDER BY name;
