WITH largest_district AS (
    SELECT district_id
    FROM district
    WHERE A3 = 'south Bohemia'
    ORDER BY CAST(A4 AS INTEGER) DESC, district_id
    LIMIT 1
)
SELECT ROUND(
    100.0 * COUNT(CASE WHEN c.gender = 'M' THEN 1 END) / NULLIF(COUNT(*), 0),
    2
) AS male_percentage
FROM client AS c
JOIN largest_district AS d ON c.district_id = d.district_id;
