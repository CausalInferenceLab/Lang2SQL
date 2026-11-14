WITH avg_salary_per_rank AS (
  SELECT rank, AVG(salary) AS avg_salary
  FROM university_faculty
  GROUP BY rank
),
faculty_with_diff AS (
  SELECT 
    uf.rank,
    uf.first_name,
    uf.last_name,
    uf.salary,
    ABS(uf.salary - asp.avg_salary) AS diff_to_avg
  FROM university_faculty uf
  JOIN avg_salary_per_rank asp ON uf.rank = asp.rank
)
SELECT rank, first_name, last_name, salary
FROM (
  SELECT 
    rank, 
    first_name, 
    last_name, 
    salary,
    diff_to_avg,
    ROW_NUMBER() OVER (PARTITION BY rank ORDER BY diff_to_avg ASC) as rn
  FROM faculty_with_diff
)
WHERE rn = 1
ORDER BY rank;
