WITH school_rates AS (
    SELECT s.School AS school_name,
           s.Charter,
           s.GSserved AS grades_served,
           CAST(f."Free Meal Count (K-12)" AS REAL)
               / NULLIF(f."Enrollment (K-12)", 0) AS rate,
           t.AvgScrRead + t.AvgScrMath + t.AvgScrWrite AS sat_total
    FROM main.schools AS s
    JOIN main.frpm AS f ON f.CDSCode = s.CDSCode
    LEFT JOIN main.satscores AS t ON t.cds = s.CDSCode
    WHERE s.County = 'Alameda'
      AND (s.GSserved = '12' OR s.GSserved LIKE '%-12')
)
SELECT school_name,
       ROUND(rate, 4) AS free_meal_rate,
       CASE Charter
           WHEN 1 THEN 'Yes'
           WHEN 0 THEN 'No'
           ELSE 'Unknown'
       END AS charter_school,
       grades_served,
       CASE
           WHEN sat_total IS NULL THEN 'No SAT Data'
           WHEN sat_total < 1200 THEN 'Below Average'
           WHEN sat_total <= 1500 THEN 'Average'
           ELSE 'Above Average'
       END AS sat_performance_level
FROM school_rates
WHERE rate = (SELECT MAX(rate) FROM school_rates)
ORDER BY school_name
LIMIT 1000
