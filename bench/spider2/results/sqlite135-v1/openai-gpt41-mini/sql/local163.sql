WITH avg_rank_salary AS (
  SELECT rank, AVG(salary) AS avg_salary
  FROM university_faculty
  GROUP BY rank
),
rank_salary_diff AS (
  SELECT f.rank, f.first_name, f.last_name, f.salary,
         ABS(f.salary - a.avg_salary) AS salary_diff,
         a.avg_salary
  FROM university_faculty f
  JOIN avg_rank_salary a ON f.rank = a.rank
)
SELECT rank, first_name, last_name, salary
FROM (
  SELECT rank, first_name, last_name, salary,
         RANK() OVER (PARTITION BY rank ORDER BY salary_diff) AS rnk
  FROM rank_salary_diff
)
WHERE rnk = 1
ORDER BY rank;
