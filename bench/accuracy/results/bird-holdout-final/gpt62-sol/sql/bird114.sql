SELECT COUNT(*) AS printing_count
FROM cards AS c
WHERE EXISTS (
    SELECT 1
    FROM sets AS s
    WHERE s.code = c.setCode
      AND s.name = 'Coldsnap'
)
AND EXISTS (
    SELECT 1
    FROM foreign_data AS f
    WHERE f.uuid = c.uuid
      AND f.language = 'Italian'
      AND TRIM(f.text) <> ''
);
