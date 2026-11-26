WITH incident_bonds AS (
    SELECT atom_id, bond_id FROM connected
    UNION ALL
    SELECT atom_id2 AS atom_id, bond_id FROM connected
), single_bond_atoms AS (
    SELECT i.atom_id
    FROM incident_bonds AS i
    JOIN bond AS b ON b.bond_id = i.bond_id
    GROUP BY i.atom_id
    HAVING MIN(CASE WHEN b.bond_type = '-' THEN 1 ELSE 0 END) = 1
)
SELECT a.element AS element_symbol,
       COUNT(s.atom_id) AS atom_count
FROM atom AS a
LEFT JOIN single_bond_atoms AS s ON s.atom_id = a.atom_id
GROUP BY a.element
ORDER BY a.element;
