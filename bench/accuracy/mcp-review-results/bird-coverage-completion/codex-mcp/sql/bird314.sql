WITH RECURSIVE over_budget_events AS (
    SELECT DISTINCT link_to_event
    FROM budget
    WHERE remaining < 0
),
items(item, rest) AS (
    SELECT NULL, e.expense_description || ','
    FROM expense AS e
    JOIN budget AS b ON b.budget_id = e.link_to_budget
    JOIN over_budget_events AS o ON o.link_to_event = b.link_to_event

    UNION ALL

    SELECT lower(trim(substr(rest, 1, instr(rest, ',') - 1))),
           substr(rest, instr(rest, ',') + 1)
    FROM items
    WHERE rest <> ''
)
SELECT COUNT(DISTINCT item) AS item_kind_count
FROM items
WHERE item <> ''
LIMIT 1
