WITH ranked_printings AS (
    SELECT s.name AS set_name,
           s.releaseDate AS release_date,
           DENSE_RANK() OVER (
               ORDER BY CASE c.frameVersion
                            WHEN 'future' THEN 2007
                            ELSE CAST(c.frameVersion AS INTEGER)
                        END DESC,
                        s.releaseDate DESC
           ) AS printing_rank
    FROM cards AS c
    JOIN sets AS s ON s.code = c.setCode
    WHERE c.name = 'Aven Mindcensor'
)
SELECT DISTINCT set_name, release_date
FROM ranked_printings
WHERE printing_rank = 1
ORDER BY set_name;
