WITH bonded_atoms(atom_id) AS (
    SELECT atom_id FROM main.connected
    UNION
    SELECT atom_id2 FROM main.connected
)
SELECT a.element, COUNT(b.atom_id) AS bonded_atom_count
FROM main.atom AS a
LEFT JOIN bonded_atoms AS b ON b.atom_id = a.atom_id
WHERE a.molecule_id = 'TR397'
GROUP BY a.element
ORDER BY a.element
LIMIT 1000
