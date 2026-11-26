SELECT ROUND(100.0 * AVG(CASE WHEN c.uuid IN (
    SELECT f.uuid
    FROM foreign_data AS f
    WHERE f.language = 'French'
) THEN 1 ELSE 0 END), 2) AS french_translation_percentage
FROM cards AS c
WHERE NULLIF(TRIM(c.power), '') IS NULL
   OR INSTR(c.power, '*') > 0;
