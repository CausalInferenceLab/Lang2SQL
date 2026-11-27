WITH frame_counts AS (
    SELECT frameVersion,
           COUNT(*) AS printing_count,
           SUM(COUNT(*)) OVER () AS total_printings
    FROM cards
    WHERE name = 'Tarmogoyf'
    GROUP BY frameVersion
)
SELECT CAST(frameVersion AS INTEGER) AS frame_year,
       ROUND(100.0 * printing_count / total_printings, 2) AS percentage
FROM frame_counts
WHERE frameVersion GLOB '[0-9][0-9][0-9][0-9]'
ORDER BY frame_year DESC
LIMIT 1;
