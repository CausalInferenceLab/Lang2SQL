WITH meal_rates AS (
    SELECT s.CDSCode,
           s.School AS school_name,
           s.Charter AS charter,
           s.GSserved AS grades_served,
           1.0 * f."Free Meal Count (K-12)" /
               NULLIF(f."Enrollment (K-12)", 0) AS free_meal_rate
    FROM schools AS s
    JOIN frpm AS f ON f.CDSCode = s.CDSCode
    WHERE s.County = 'Alameda'
      AND (TRIM(s.GSserved) = '12' OR TRIM(s.GSserved) LIKE '%-12')
), top_schools AS (
    SELECT CDSCode, school_name, charter, grades_served, free_meal_rate
    FROM meal_rates
    WHERE free_meal_rate = (SELECT MAX(free_meal_rate) FROM meal_rates)
), results AS (
    SELECT m.school_name, m.free_meal_rate, m.charter, m.grades_served,
           t.AvgScrRead + t.AvgScrMath + t.AvgScrWrite AS sat_total
    FROM top_schools AS m
    LEFT JOIN satscores AS t ON t.cds = m.CDSCode
)
SELECT school_name,
       ROUND(free_meal_rate, 4) AS free_meal_rate,
       CASE charter WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' ELSE 'Unknown' END AS charter_school,
       grades_served,
       CASE
           WHEN sat_total IS NULL THEN 'No SAT Data'
           WHEN sat_total < 1200 THEN 'Below Average'
           WHEN sat_total <= 1500 THEN 'Average'
           ELSE 'Above Average'
       END AS sat_performance_level
FROM results
ORDER BY school_name;
