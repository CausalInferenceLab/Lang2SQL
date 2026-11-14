WITH relevant_postings AS (
  SELECT job_id
  FROM job_postings_fact
  WHERE job_title = 'Data Analyst'
    AND salary_year_avg IS NOT NULL
    AND job_work_from_home = 1
),
skill_counts AS (
  SELECT skill_id, COUNT(*) AS freq
  FROM skills_job_dim
  WHERE job_id IN (SELECT job_id FROM relevant_postings)
  GROUP BY skill_id
  ORDER BY freq DESC
  LIMIT 3
),
top_skills_postings AS (
  SELECT DISTINCT job_id
  FROM skills_job_dim
  WHERE skill_id IN (SELECT skill_id FROM skill_counts)
),
filtered_postings AS (
  SELECT salary_year_avg
  FROM job_postings_fact
  WHERE job_id IN (SELECT job_id FROM top_skills_postings)
    AND job_title = 'Data Analyst'
    AND salary_year_avg IS NOT NULL
    AND job_work_from_home = 1
)
SELECT AVG(salary_year_avg) AS overall_avg_salary
FROM filtered_postings
;
