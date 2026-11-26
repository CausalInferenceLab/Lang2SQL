WITH fastest_lap_constructors AS (
    SELECT DISTINCT constructorId
    FROM results
    WHERE rank = 1
)
SELECT
    c.nationality,
    COUNT(DISTINCT f.constructorId) AS constructor_count
FROM constructors AS c
LEFT JOIN fastest_lap_constructors AS f
    ON f.constructorId = c.constructorId
GROUP BY c.nationality
ORDER BY c.nationality;
