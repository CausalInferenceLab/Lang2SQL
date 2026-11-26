SELECT DISTINCT c.bond_id
FROM main.connected AS c
JOIN main.atom AS a1 ON a1.atom_id = c.atom_id
JOIN main.atom AS a2 ON a2.atom_id = c.atom_id2
WHERE (a1.element = 'p' AND a2.element = 'n')
   OR (a1.element = 'n' AND a2.element = 'p')
ORDER BY c.bond_id
LIMIT -1
