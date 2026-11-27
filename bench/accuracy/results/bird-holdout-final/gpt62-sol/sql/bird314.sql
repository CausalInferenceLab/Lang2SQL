WITH RECURSIVE over_budget_events AS (
    SELECT DISTINCT link_to_event
    FROM budget
    WHERE amount - spent < 0
), purchase_items(rest, item) AS (
    SELECT x.expense_description || ',', NULL
    FROM over_budget_events AS e
    JOIN budget AS b ON b.link_to_event = e.link_to_event
    JOIN expense AS x ON x.link_to_budget = b.budget_id
    WHERE x.expense_description IS NOT NULL

    UNION ALL

    SELECT substr(rest, instr(rest, ',') + 1),
           lower(trim(substr(rest, 1, instr(rest, ',') - 1)))
    FROM purchase_items
    WHERE rest <> ''
)
SELECT COUNT(DISTINCT item) AS item_kind_count
FROM purchase_items
WHERE item <> '';
