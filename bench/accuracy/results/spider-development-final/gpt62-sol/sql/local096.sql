WITH female_only_casts AS (
    SELECT c.MID
    FROM M_Cast AS c
    LEFT JOIN Person AS p ON p.PID = TRIM(c.PID)
    GROUP BY c.MID
    HAVING MIN(CASE WHEN p.Gender = 'Female' THEN 1 ELSE 0 END) = 1
)
SELECT CAST(SUBSTR(m.year, -4) AS INTEGER) AS year,
       COUNT(*) AS total_movies,
       100.0 * COUNT(f.MID) / COUNT(*) AS percentage_exclusively_female
FROM Movie AS m
LEFT JOIN female_only_casts AS f ON f.MID = m.MID
GROUP BY CAST(SUBSTR(m.year, -4) AS INTEGER)
ORDER BY year;
