WITH bond_counts AS (
    SELECT bond_type, COUNT(*) AS bond_count
    FROM bond
    WHERE molecule_id = 'TR010'
    GROUP BY bond_type
)
SELECT bc.bond_type,
       CASE m.label WHEN '+' THEN 'YES' WHEN '-' THEN 'NO' END AS carcinogenic
FROM molecule AS m
CROSS JOIN bond_counts AS bc
WHERE m.molecule_id = 'TR010'
  AND bc.bond_count = (SELECT MAX(bond_count) FROM bond_counts);
