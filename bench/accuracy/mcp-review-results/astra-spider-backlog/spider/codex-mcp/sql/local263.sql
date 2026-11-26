WITH model_statuses AS (
    SELECT DISTINCT name, version, L1_model, status
    FROM main.stack_ok
    WHERE status IN ('strong', 'soft')
),
ranked_counts AS (
    SELECT status, L1_model, COUNT(*) AS occurrences,
           RANK() OVER (PARTITION BY status ORDER BY COUNT(*) DESC) AS frequency_rank
    FROM model_statuses
    GROUP BY status, L1_model
)
SELECT status, L1_model, occurrences
FROM ranked_counts
WHERE frequency_rank = 1
ORDER BY status, L1_model
LIMIT 1000
