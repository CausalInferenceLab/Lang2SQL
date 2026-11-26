SELECT L1_model, COUNT(*) AS total_occurrences
FROM stack_ok
GROUP BY L1_model
ORDER BY total_occurrences DESC, L1_model
LIMIT 1;
