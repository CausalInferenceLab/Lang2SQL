WITH event_totals AS (
    SELECT e.event_id,
           COALESCE(SUM(CAST(ROUND(b.spent * 100) AS INTEGER)), 0) AS spent_cents
    FROM main.event AS e
    LEFT JOIN main.budget AS b ON b.link_to_event = e.event_id
    GROUP BY e.event_id
)
SELECT ROUND(AVG(spent_cents)) / 100.0 AS average_spent_per_event_dollars
FROM event_totals
LIMIT 1
