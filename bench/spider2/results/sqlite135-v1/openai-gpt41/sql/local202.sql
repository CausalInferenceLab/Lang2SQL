WITH state_alien_stats AS (
    SELECT
        l.state AS state,
        COUNT(a.alien_id) AS alien_population,
        SUM(CASE WHEN d.disposition = 'friendly' THEN 1 ELSE 0 END) * 1.0 / COUNT(a.alien_id) AS pct_friendly,
        SUM(CASE WHEN d.disposition = 'hostile' THEN 1 ELSE 0 END) * 1.0 / COUNT(a.alien_id) AS pct_hostile,
        AVG(d.age) AS avg_alien_age
    FROM aliens a
    JOIN aliens_details d ON a.alien_id = d.alien_id
    JOIN aliens_location l ON a.alien_id = l.alien_id
    GROUP BY l.state
),
top_states AS (
    SELECT *
    FROM state_alien_stats
    ORDER BY alien_population DESC
    LIMIT 10
)
SELECT COUNT(*) AS num_states
FROM top_states
WHERE pct_friendly > pct_hostile
  AND avg_alien_age > 200;
