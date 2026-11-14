WITH TopStates AS (
    SELECT state, COUNT(*) AS population
    FROM alien_data
    GROUP BY state
    ORDER BY population DESC
    LIMIT 10
),
AlienStats AS (
    SELECT
        state,
        AVG(age) AS avg_age,
        SUM(CASE WHEN aggressive = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS friendly_ratio,
        SUM(CASE WHEN aggressive = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS hostile_ratio
    FROM alien_data
    WHERE state IN (SELECT state FROM TopStates)
    GROUP BY state
)
SELECT COUNT(*) AS count_top_states
FROM AlienStats
WHERE friendly_ratio > hostile_ratio AND avg_age > 200;
