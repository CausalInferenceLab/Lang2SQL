WITH abzan_cards AS (
    SELECT c.uuid, c.name, c.number,
           RANK() OVER (ORDER BY CAST(c.number AS INTEGER) DESC) AS collector_rank
    FROM cards AS c
    WHERE c.watermark = 'abzan' COLLATE NOCASE
)
SELECT a.name,
       a.number AS collector_number,
       (SELECT COUNT(*) FROM foreign_data AS f WHERE f.uuid = a.uuid) AS foreign_language_printings
FROM abzan_cards AS a
WHERE a.collector_rank = 1
ORDER BY a.name;
