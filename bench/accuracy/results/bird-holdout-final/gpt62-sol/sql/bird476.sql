SELECT COUNT(*) AS atom_count
FROM atom AS a
WHERE EXISTS (
    SELECT 1
    FROM bond AS b
    WHERE b.molecule_id = a.molecule_id
      AND b.bond_type = '#'
)
AND EXISTS (
    SELECT 1
    FROM atom AS pb
    WHERE pb.molecule_id = a.molecule_id
      AND pb.element IN ('p', 'br')
);
