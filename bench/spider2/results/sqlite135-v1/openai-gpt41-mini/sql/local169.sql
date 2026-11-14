WITH cohort AS (
  SELECT id_bioguide, MIN(term_start) AS first_term_start
  FROM legislators_terms
  WHERE term_start BETWEEN '1917-01-01' AND '1999-12-31'
  GROUP BY id_bioguide
),
term_years AS (
  SELECT c.id_bioguide, c.first_term_start, 
         term_start, term_end
  FROM legislators_terms t
  JOIN cohort c ON t.id_bioguide = c.id_bioguide
),
retention AS (
  SELECT y.year,
         COUNT(DISTINCT cohort.id_bioguide) AS cohort_size,
         COUNT(DISTINCT CASE 
           WHEN ty.term_start <= date(cohort.first_term_start, printf('+%d years', y.year)) 
                AND (ty.term_end >= date(cohort.first_term_start, printf('+%d years', y.year)) OR ty.term_end IS NULL)
           THEN cohort.id_bioguide
         END) AS retained_count
  FROM 
    (SELECT 1 AS year UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
     UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
     UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20) y
  CROSS JOIN cohort
  LEFT JOIN term_years ty ON ty.id_bioguide = cohort.id_bioguide
  GROUP BY y.year
  ORDER BY y.year
)
SELECT 
  year,
  retained_count * 1.0 / cohort_size AS annual_retention_rate
FROM retention;
