SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE m.label = '+') / NULLIF(COUNT(*), 0), 2) AS carcinogenic_percent,
    ROUND(100.0 * COUNT(*) FILTER (WHERE m.label = '-') / NULLIF(COUNT(*), 0), 2) AS non_carcinogenic_percent
FROM main.molecule AS m
WHERE m.molecule_id IN (
    SELECT b.molecule_id
    FROM main.bond AS b
    WHERE b.bond_type = '#'
)
LIMIT 1
