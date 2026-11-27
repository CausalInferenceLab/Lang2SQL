WITH single_bond_atoms AS (
    SELECT c.atom_id AS atom_id
    FROM connected AS c
    JOIN bond AS b ON b.bond_id = c.bond_id
    WHERE b.bond_type = '-'
    UNION
    SELECT c.atom_id2 AS atom_id
    FROM connected AS c
    JOIN bond AS b ON b.bond_id = c.bond_id
    WHERE b.bond_type = '-'
)
SELECT
    COUNT(DISTINCT CASE WHEN a.element = 'i' THEN a.atom_id END) AS iodine_atoms,
    COUNT(DISTINCT CASE WHEN a.element = 's' THEN a.atom_id END) AS sulfur_atoms
FROM atom AS a
JOIN single_bond_atoms AS s ON s.atom_id = a.atom_id
WHERE a.element IN ('i', 's');
