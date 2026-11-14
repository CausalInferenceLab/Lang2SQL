WITH stack_scores AS (
  SELECT
    ms.name,
    ms.version,
    ms.step,
    ms.L1_model,
    ms.test_score AS stack_test_score
  FROM model_score ms
  WHERE ms.L1_model = 'Stack'
),
nonstack_max_scores AS (
  SELECT
    ms.name,
    ms.version,
    ms.step,
    MAX(ms.test_score) AS nonstack_max_test_score
  FROM model_score ms
  WHERE ms.L1_model != 'Stack'
  GROUP BY ms.name, ms.version, ms.step
),
model_status AS (
  SELECT
    s.name,
    s.version,
    s.step,
    s.stack_test_score,
    n.nonstack_max_test_score,
    CASE
      WHEN n.nonstack_max_test_score < s.stack_test_score THEN 'strong'
      WHEN n.nonstack_max_test_score = s.stack_test_score THEN 'soft'
      ELSE NULL
    END AS status
  FROM stack_scores s
  JOIN nonstack_max_scores n
    ON s.name = n.name AND s.version = n.version AND s.step = n.step
),
l1_status_counts AS (
  SELECT
    ms.name,
    ms.version,
    ms.step,
    m.L1_model,
    ms.status
  FROM model_status ms
  JOIN model_score m
    ON ms.name = m.name AND ms.version = m.version AND ms.step = m.step
  WHERE ms.status IN ('strong', 'soft')
    AND m.L1_model != 'Stack'
),
l1_status_totals AS (
  SELECT
    L1_model,
    status,
    COUNT(*) AS occurrence_count
  FROM l1_status_counts
  GROUP BY L1_model, status
),
ranked_l1 AS (
  SELECT
    L1_model,
    status,
    occurrence_count,
    RANK() OVER (PARTITION BY status ORDER BY occurrence_count DESC) AS rnk
  FROM l1_status_totals
)
SELECT
  status,
  L1_model,
  occurrence_count
FROM ranked_l1
WHERE rnk = 1
ORDER BY status;
