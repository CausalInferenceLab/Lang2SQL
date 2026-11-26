WITH element_counts AS (
    SELECT a.element, COUNT(*) AS occurrences
    FROM atom AS a
    WHERE EXISTS (
        SELECT 1
        FROM molecule AS m
        WHERE m.molecule_id = a.molecule_id
          AND m.label = '-'
    )
    GROUP BY a.element
)
SELECT element
FROM element_counts
WHERE occurrences = (SELECT MIN(occurrences) FROM element_counts)
ORDER BY element;
