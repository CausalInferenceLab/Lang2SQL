WITH eligible_jobs AS (
    SELECT job_id, salary_year_avg
    FROM job_postings_fact
    WHERE job_title_short = 'Data Analyst'
      AND salary_year_avg IS NOT NULL
      AND job_work_from_home = 1
), top_skills AS (
    SELECT sj.skill_id
    FROM skills_job_dim AS sj
    JOIN eligible_jobs AS j ON j.job_id = sj.job_id
    GROUP BY sj.skill_id
    ORDER BY COUNT(DISTINCT sj.job_id) DESC, sj.skill_id
    LIMIT 3
)
SELECT AVG(j.salary_year_avg) AS overall_avg_salary
FROM eligible_jobs AS j
WHERE EXISTS (
    SELECT 1
    FROM skills_job_dim AS sj
    JOIN top_skills AS ts ON ts.skill_id = sj.skill_id
    WHERE sj.job_id = j.job_id
);
