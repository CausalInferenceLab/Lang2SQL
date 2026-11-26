WITH oxygen_counts AS (
    SELECT m.molecule_id, COUNT(a.atom_id) AS oxygen_atoms
    FROM main.molecule AS m
    LEFT JOIN main.atom AS a
        ON a.molecule_id = m.molecule_id AND a.element = 'o'
    WHERE m.molecule_id IN (
        SELECT b.molecule_id
        FROM main.bond AS b
        WHERE b.bond_type = '-'
    )
    GROUP BY m.molecule_id
)
SELECT ROUND(AVG(oxygen_atoms), 4) AS average_oxygen_atoms
FROM oxygen_counts
LIMIT 1
