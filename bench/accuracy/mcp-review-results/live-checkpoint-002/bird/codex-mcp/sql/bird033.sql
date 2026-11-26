SELECT COUNT(School) / 12.0 AS average_schools_per_month
FROM main.schools
WHERE County = 'San Diego'
  AND OpenDate >= '1980-01-01'
  AND OpenDate < '1981-01-01'
  AND DOCType IN ('Elementary School District', 'High School District', 'Unified School District')
LIMIT 1
