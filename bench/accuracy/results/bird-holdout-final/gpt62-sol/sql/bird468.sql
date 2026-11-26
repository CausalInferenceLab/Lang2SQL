SELECT ROUND(AVG(oxygen_count), 4) AS avg_oxygen_atoms_per_molecule
FROM (
    SELECT m.molecule_id, COUNT(a.atom_id) AS oxygen_count
    FROM molecule AS m
    LEFT JOIN atom AS a
        ON a.molecule_id = m.molecule_id AND a.element = 'o'
    WHERE EXISTS (
        SELECT 1
        FROM bond AS b
        WHERE b.molecule_id = m.molecule_id AND b.bond_type = '-'
    )
    GROUP BY m.molecule_id
) AS molecule_oxygen_counts;
