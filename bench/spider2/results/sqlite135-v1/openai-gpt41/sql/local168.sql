WITH filtered_jobs AS (
  SELECT j.job_id
  FROM job_postings_fact j
  WHERE j.job_title_short = 'Data Analyst'
    AND j.salary_year_avg IS NOT NULL
    AND j.job_work_from_home = 1
),
skills_frequency AS (
  SELECT s.skill_id, COUNT(*) AS freq
  FROM filtered_jobs f
  JOIN skills_job_dim s ON f.job_id = s.job_id
  GROUP BY s.skill_id
  ORDER BY freq DESC
  LIMIT 3
),
qualified_jobs AS (
  SELECT DISTINCT s.job_id
  FROM skills_job_dim s
  WHERE s.skill_id IN (SELECT skill_id FROM skills_frequency)
    AND s.job_id IN (SELECT job_id FROM filtered_jobs)
),
final_salary AS (
  SELECT j.salary_year_avg
  FROM qualified_jobs q
  JOIN job_postings_fact j ON q.job_id = j.job_id
)
SELECT AVG(salary_year_avg) AS overall_avg_salary
FROM final_salary
