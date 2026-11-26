WITH fluorinated_molecules AS (
    SELECT DISTINCT a.molecule_id
    FROM main.atom AS a
    WHERE a.element = 'f'
)
SELECT ROUND(
    100.0 * (COUNT(*) - COUNT(f.molecule_id)) / NULLIF(COUNT(*), 0),
    2
) AS percentage_without_fluorine
FROM main.molecule AS m
LEFT JOIN fluorinated_molecules AS f ON f.molecule_id = m.molecule_id
WHERE m.label = '+'
LIMIT 1
