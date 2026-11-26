WITH printings AS (
    SELECT s.name AS set_name,
           s.releaseDate,
           CASE
               WHEN c.frameVersion GLOB '[0-9][0-9][0-9][0-9]'
                   THEN CAST(c.frameVersion AS INTEGER)
               -- The 'future' frame was already in use by 2007, before the 2015 frame.
               ELSE MIN(CAST(strftime('%Y', s.releaseDate) AS INTEGER))
                    OVER (PARTITION BY c.frameVersion)
           END AS frame_year_upper_bound
    FROM main.cards AS c
    JOIN main.sets AS s ON s.code = c.setCode
    WHERE c.name = 'Aven Mindcensor'
),
ranked AS (
    SELECT set_name, releaseDate,
           RANK() OVER (
               ORDER BY frame_year_upper_bound DESC, releaseDate DESC
           ) AS printing_rank
    FROM printings
)
SELECT DISTINCT set_name, releaseDate
FROM ranked
WHERE printing_rank = 1
ORDER BY set_name
LIMIT 1000
