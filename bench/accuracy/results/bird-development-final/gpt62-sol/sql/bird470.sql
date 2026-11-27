WITH element_counts AS (
    SELECT a.element, COUNT(*) AS atom_count
    FROM atom AS a
    JOIN molecule AS m ON m.molecule_id = a.molecule_id
    WHERE m.label = '-'
    GROUP BY a.element
)
SELECT element
FROM element_counts
WHERE atom_count = (SELECT MIN(atom_count) FROM element_counts);
