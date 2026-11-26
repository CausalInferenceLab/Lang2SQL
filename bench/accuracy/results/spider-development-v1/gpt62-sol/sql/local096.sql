WITH cast_status AS (
    SELECT c.MID,
           MIN(CASE WHEN p.Gender = 'Female' THEN 1 ELSE 0 END) AS all_female
    FROM M_Cast AS c
    LEFT JOIN Person AS p ON p.PID = TRIM(c.PID)
    GROUP BY c.MID
)
SELECT CAST(SUBSTR(m.year, -4) AS INTEGER) AS year,
       COUNT(*) AS total_movies,
       100.0 * COUNT(CASE WHEN cs.all_female = 1 THEN 1 END) / COUNT(*)
           AS percentage_exclusively_female
FROM Movie AS m
LEFT JOIN cast_status AS cs ON cs.MID = m.MID
GROUP BY CAST(SUBSTR(m.year, -4) AS INTEGER)
ORDER BY year;
