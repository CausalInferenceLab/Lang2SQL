WITH first_month AS (
    SELECT DATE(MIN(date), 'start of month') AS month_start
    FROM account
), eligible_districts AS (
    SELECT DISTINCT a.district_id
    FROM account AS a
    CROSS JOIN first_month AS f
    WHERE a.date >= f.month_start
      AND a.date < DATE(f.month_start, '+1 month')
), populations AS (
    SELECT d.A2 AS district_name, CAST(d.A4 AS INTEGER) AS population
    FROM eligible_districts AS e
    JOIN district AS d ON d.district_id = e.district_id
)
SELECT district_name, population
FROM populations
WHERE population = (SELECT MAX(population) FROM populations)
ORDER BY district_name;
