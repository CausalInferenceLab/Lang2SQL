SELECT ROUND(100.0 * COUNT(CASE WHEN a.element = 'cl' THEN 1 END) / COUNT(*), 4) AS chlorine_percentage
FROM atom AS a
WHERE EXISTS (
    SELECT 1
    FROM bond AS b
    WHERE b.molecule_id = a.molecule_id
      AND b.bond_type = '-'
);
