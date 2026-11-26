SELECT ROUND(
    100.0 * COUNT(*) FILTER (WHERE c.gender = 'M') / NULLIF(COUNT(*), 0),
    2
) AS male_percentage
FROM main.client AS c
WHERE c.district_id = (
    SELECT d.district_id
    FROM main.district AS d
    WHERE d.A3 = 'south Bohemia'
    ORDER BY CAST(d.A4 AS INTEGER) DESC
    LIMIT 1
)
LIMIT 1000
