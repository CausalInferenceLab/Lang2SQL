SELECT DISTINCT a.element
FROM atom AS a
WHERE NOT EXISTS (
    SELECT 1
    FROM connected AS c
    WHERE c.atom_id = a.atom_id OR c.atom_id2 = a.atom_id
);
