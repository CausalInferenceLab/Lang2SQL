SELECT DISTINCT a.element
FROM main.atom AS a
WHERE a.atom_id NOT IN (
    SELECT c.atom_id FROM main.connected AS c
    UNION
    SELECT c.atom_id2 FROM main.connected AS c
)
LIMIT 1000
