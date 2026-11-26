WITH model_statuses AS (
    SELECT DISTINCT name, version, L1_model, status
    FROM stack_ok
), status_counts AS (
    SELECT status, L1_model, COUNT(*) AS occurrence_count
    FROM model_statuses
    GROUP BY status, L1_model
), ranked AS (
    SELECT status, L1_model, occurrence_count,
           DENSE_RANK() OVER (PARTITION BY status ORDER BY occurrence_count DESC) AS status_rank
    FROM status_counts
)
SELECT status, L1_model, occurrence_count
FROM ranked
WHERE status_rank = 1
ORDER BY status, L1_model;
