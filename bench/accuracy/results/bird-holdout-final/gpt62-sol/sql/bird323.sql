WITH event_totals AS (
    SELECT e.event_id, COALESCE(SUM(b.spent), 0.0) AS total_spent
    FROM event AS e
    LEFT JOIN budget AS b ON b.link_to_event = e.event_id
    GROUP BY e.event_id
)
SELECT ROUND(AVG(total_spent), 2) AS average_spent_per_event_dollars
FROM event_totals;
