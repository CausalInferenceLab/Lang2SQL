WITH event_ratios AS (
    SELECT e.event_name,
           1.0 * SUM(b.spent) / NULLIF(SUM(b.amount), 0) AS spending_ratio
    FROM event AS e
    JOIN budget AS b ON b.link_to_event = e.event_id
    WHERE e.status = 'Closed'
    GROUP BY e.event_id, e.event_name
)
SELECT event_name
FROM event_ratios
WHERE spending_ratio = (SELECT MAX(spending_ratio) FROM event_ratios)
ORDER BY event_name;
