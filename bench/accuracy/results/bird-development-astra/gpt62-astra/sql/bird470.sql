WITH element_counts AS (
    SELECT a.element, COUNT(*) AS occurrences
    FROM atom AS a
    JOIN molecule AS m ON m.molecule_id = a.molecule_id
    WHERE m.label = '-'
    GROUP BY a.element
)
SELECT element
FROM element_counts
WHERE occurrences = (SELECT MIN(occurrences) FROM element_counts)
ORDER BY element;
