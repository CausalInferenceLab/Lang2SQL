WITH sulfur_atoms AS (
  SELECT a.atom_id
  FROM atom AS a
  JOIN molecule AS m ON m.molecule_id = a.molecule_id
  WHERE a.element = 's' AND m.label = '+'
), bonded_atoms AS (
  SELECT c.atom_id2 AS atom_id
  FROM connected AS c
  JOIN sulfur_atoms AS s ON s.atom_id = c.atom_id
  UNION
  SELECT c.atom_id
  FROM connected AS c
  JOIN sulfur_atoms AS s ON s.atom_id = c.atom_id2
)
SELECT COUNT(*) AS atom_count
FROM bonded_atoms;
