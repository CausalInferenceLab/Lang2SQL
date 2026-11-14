WITH stack_vs_nonstack AS (
  SELECT
    m1.problem_id,
    m1.version,
    m1.step,
    MAX(CASE WHEN m1.model_name = 'Stack' THEN ms1.test_score ELSE NULL END) AS stack_test_score,
    MAX(CASE WHEN m1.model_name != 'Stack' THEN ms1.test_score ELSE NULL END) AS max_nonstack_score
  FROM
    model m1
    LEFT JOIN model_score ms1 ON m1.model_id = ms1.model_id
  WHERE
    m1.step IN (1,2,3)
  GROUP BY
    m1.problem_id, m1.version, m1.step
),
problems_to_include AS (
  SELECT DISTINCT
    problem_id
  FROM
    stack_vs_nonstack
  WHERE
    stack_test_score IS NOT NULL
    AND max_nonstack_score IS NOT NULL
    AND max_nonstack_score < stack_test_score
),
problem_solution_occurrences AS (
  SELECT
    s.problem_id,
    COUNT(*) AS total_solution_occurrences
  FROM
    solution s
  GROUP BY
    s.problem_id
)
SELECT
  p.problem_id
FROM
  problems_to_include p
  LEFT JOIN problem_solution_occurrences ps ON p.problem_id = ps.problem_id
WHERE
  ps.total_solution_occurrences > (
    SELECT COUNT(*) FROM solution s2 WHERE s2.problem_id = p.problem_id
  )
;
