WITH event_maxima AS (
    SELECT b.link_to_event AS event_id, MAX(b.amount) AS largest_line
    FROM main.budget AS b
    WHERE b.link_to_event IN (
        SELECT chip_budget.link_to_event
        FROM main.expense AS x
        JOIN main.budget AS chip_budget ON chip_budget.budget_id = x.link_to_budget
        WHERE x.expense_description LIKE '%chips%' AND x.cost > 0
    )
    GROUP BY b.link_to_event
)
SELECT e.event_name
FROM main.event AS e
JOIN event_maxima AS m ON m.event_id = e.event_id
WHERE m.largest_line = (SELECT MAX(largest_line) FROM event_maxima)
ORDER BY e.event_name
LIMIT -1
