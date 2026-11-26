WITH eligible AS (
    SELECT s.CDSCode, s.School, s.Charter, s.GSserved,
           1.0 * f."Free Meal Count (K-12)" / NULLIF(f."Enrollment (K-12)", 0) AS meal_rate
    FROM schools AS s
    JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.County = 'Alameda'
      AND (TRIM(s.GSserved) = '12' OR TRIM(s.GSserved) LIKE '%-12')
), winners AS (
    SELECT e.*, sat.AvgScrRead + sat.AvgScrMath + sat.AvgScrWrite AS sat_total
    FROM eligible AS e
    LEFT JOIN satscores AS sat ON sat.cds = e.CDSCode
    WHERE e.meal_rate = (SELECT MAX(meal_rate) FROM eligible)
)
SELECT School AS school_name,
       ROUND(meal_rate, 4) AS free_meal_rate,
       CASE Charter WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE 'Unknown' END AS charter_school,
       GSserved AS grades_served,
       CASE
           WHEN sat_total IS NULL THEN 'No SAT Data'
           WHEN sat_total < 1200 THEN 'Below Average'
           WHEN sat_total <= 1500 THEN 'Average'
           ELSE 'Above Average'
       END AS sat_performance_level
FROM winners
ORDER BY school_name;
