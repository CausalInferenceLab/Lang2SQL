SELECT COUNT(DISTINCT b.bond_id) AS hydrogen_bond_count
FROM bond AS b
WHERE EXISTS (
    SELECT 1
    FROM atom AS y
    WHERE y.molecule_id = b.molecule_id AND y.element = 'y'
)
AND EXISTS (
    SELECT 1
    FROM connected AS c
    JOIN atom AS h ON h.atom_id IN (c.atom_id, c.atom_id2)
    WHERE c.bond_id = b.bond_id
      AND h.molecule_id = b.molecule_id
      AND h.element = 'h'
);
