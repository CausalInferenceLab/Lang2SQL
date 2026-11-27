WITH film_flags AS (
    SELECT m.MID,
           CAST(SUBSTR(m.year, -4) AS INTEGER) AS film_year,
           MIN(CASE WHEN p.Gender = 'Female' THEN 1 ELSE 0 END) AS exclusively_female
    FROM Movie AS m
    LEFT JOIN M_Cast AS c ON c.MID = m.MID
    LEFT JOIN Person AS p ON p.PID = TRIM(c.PID)
    GROUP BY m.MID, CAST(SUBSTR(m.year, -4) AS INTEGER)
)
SELECT film_year AS year,
       COUNT(*) AS total_movies,
       100.0 * SUM(exclusively_female) / COUNT(*) AS exclusively_female_pct
FROM film_flags
GROUP BY film_year
ORDER BY film_year;
