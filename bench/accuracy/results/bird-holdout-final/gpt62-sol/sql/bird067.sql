WITH factory_rulings AS (
    SELECT DATE(r.date) AS ruling_date, r.text AS ruling_text
    FROM rulings AS r
    WHERE EXISTS (
        SELECT 1
        FROM cards AS c
        WHERE c.uuid = r.uuid
          AND c.name = 'Mishra''s Factory'
    )
)
SELECT COUNT(DISTINCT ruling_text) AS ruling_count
FROM factory_rulings
WHERE ruling_date = (SELECT MAX(ruling_date) FROM factory_rulings);
