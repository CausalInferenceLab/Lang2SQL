WITH chip_events AS (
    SELECT DISTINCT b.link_to_event AS event_id
    FROM expense AS x
    JOIN budget AS b ON b.budget_id = x.link_to_budget
    WHERE LOWER(x.expense_description) LIKE '%chips%'
      AND x.cost > 0
), event_maxima AS (
    SELECT e.event_id, e.event_name, MAX(b.amount) AS largest_line
    FROM event AS e
    JOIN chip_events AS c ON c.event_id = e.event_id
    JOIN budget AS b ON b.link_to_event = e.event_id
    GROUP BY e.event_id, e.event_name
)
SELECT event_name
FROM event_maxima
WHERE largest_line = (SELECT MAX(largest_line) FROM event_maxima)
ORDER BY event_name;
