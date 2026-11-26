SELECT ROUND(
    100.0 * COUNT(CASE WHEN a.element = 'cl' THEN 1 END) / NULLIF(COUNT(*), 0),
    4
) AS chlorine_percentage
FROM main.atom AS a
WHERE a.molecule_id IN (
    SELECT b.molecule_id
    FROM main.bond AS b
    WHERE b.bond_type = '-'
)
LIMIT 1000
