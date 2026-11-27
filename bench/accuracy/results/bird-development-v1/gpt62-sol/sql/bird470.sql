WITH element_counts AS (
    SELECT a.element, COUNT(*) AS atom_occurrences
    FROM atom AS a
    JOIN molecule AS m ON m.molecule_id = a.molecule_id
    WHERE m.label = '-' AND a.element IS NOT NULL
    GROUP BY a.element
)
SELECT element
FROM element_counts
WHERE atom_occurrences = (SELECT MIN(atom_occurrences) FROM element_counts)
ORDER BY element;
