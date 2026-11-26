WITH event_ratios AS (
    SELECT e.event_id,
           e.event_name,
           SUM(CAST(ROUND(b.spent * 100) AS INTEGER))
               / NULLIF(100.0 * SUM(b.amount), 0) AS spending_to_budget_ratio
    FROM main.event AS e
    JOIN main.budget AS b ON b.link_to_event = e.event_id
    WHERE e.status = 'Closed'
    GROUP BY e.event_id, e.event_name
)
SELECT event_id, event_name, spending_to_budget_ratio
FROM event_ratios
WHERE spending_to_budget_ratio = (SELECT MAX(spending_to_budget_ratio) FROM event_ratios)
ORDER BY event_name, event_id
LIMIT 1000
