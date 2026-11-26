WITH top_states AS (
    SELECT state,
           AVG(age) AS average_age,
           SUM(CASE WHEN aggressive = 0 THEN 1 ELSE 0 END) AS friendly_count,
           SUM(CASE WHEN aggressive = 1 THEN 1 ELSE 0 END) AS hostile_count
    FROM alien_data
    GROUP BY state
    ORDER BY COUNT(*) DESC, state
    LIMIT 10
)
SELECT COUNT(*) AS qualifying_states
FROM top_states
WHERE friendly_count > hostile_count
  AND average_age > 200;
