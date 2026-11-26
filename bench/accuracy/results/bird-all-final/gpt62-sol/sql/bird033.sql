SELECT COUNT(*) / 12.0 AS average_schools_per_month
FROM schools
WHERE County = 'San Diego'
  AND DOC IN ('52', '54', '56')
  AND School IS NOT NULL
  AND OpenDate >= '1980-01-01'
  AND OpenDate < '1981-01-01';
