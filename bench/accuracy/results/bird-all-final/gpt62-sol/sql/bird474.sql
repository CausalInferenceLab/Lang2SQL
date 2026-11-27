SELECT DISTINCT b.bond_id
FROM bond AS b
JOIN connected AS c ON c.bond_id = b.bond_id
JOIN atom AS a1 ON a1.atom_id = c.atom_id
JOIN atom AS a2 ON a2.atom_id = c.atom_id2
WHERE (a1.element = 'p' AND a2.element = 'n')
   OR (a1.element = 'n' AND a2.element = 'p');
