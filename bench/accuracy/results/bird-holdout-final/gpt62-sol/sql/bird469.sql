WITH single_bond_counts AS (
    SELECT b.molecule_id, COUNT(*) AS single_bond_count
    FROM bond AS b
    WHERE b.bond_type = '-'
      AND EXISTS (
          SELECT 1
          FROM molecule AS m
          WHERE m.molecule_id = b.molecule_id
            AND m.label = '+'
      )
    GROUP BY b.molecule_id
)
SELECT AVG(single_bond_count) AS average_single_bonds_per_molecule
FROM single_bond_counts;
