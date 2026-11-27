WITH unique_elements AS (
    SELECT MIN(molecule_id) AS molecule_id
    FROM atom
    WHERE element IS NOT NULL
    GROUP BY element
    HAVING COUNT(DISTINCT molecule_id) = 1
)
SELECT b.bond_id, b.molecule_id
FROM bond AS b
JOIN molecule AS m ON m.molecule_id = b.molecule_id
WHERE m.label = '+'
  AND EXISTS (
      SELECT 1
      FROM unique_elements AS u
      WHERE u.molecule_id = b.molecule_id
  );
