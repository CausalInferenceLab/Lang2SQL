SELECT COUNT(DISTINCT m.molecule_id) AS molecule_count
FROM molecule AS m
WHERE EXISTS (
    SELECT 1
    FROM atom AS a
    WHERE a.molecule_id = m.molecule_id
      AND a.element = 'n'
)
AND NOT EXISTS (
    SELECT 1
    FROM atom AS a
    JOIN connected AS c ON a.atom_id IN (c.atom_id, c.atom_id2)
    JOIN bond AS b ON b.bond_id = c.bond_id
    WHERE a.molecule_id = m.molecule_id
      AND a.element = 'n'
      AND b.bond_type IN ('=', '#')
);
