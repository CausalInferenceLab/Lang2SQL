SELECT a.element,
       COUNT(DISTINCT CASE WHEN EXISTS (
           SELECT 1
           FROM connected AS c
           JOIN bond AS b ON b.bond_id = c.bond_id
           WHERE c.atom_id = a.atom_id OR c.atom_id2 = a.atom_id
       ) THEN a.atom_id END) AS bonded_atom_count
FROM atom AS a
WHERE a.molecule_id = 'TR397'
GROUP BY a.element
ORDER BY a.element;
