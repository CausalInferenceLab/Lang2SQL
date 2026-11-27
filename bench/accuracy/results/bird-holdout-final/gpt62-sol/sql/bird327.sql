WITH RECURSIVE purchase_items(expense_id, category, cost, item_name, rest) AS (
    SELECT ex.expense_id, b.category, ex.cost, '', ex.expense_description || ','
    FROM expense AS ex
    JOIN budget AS b ON b.budget_id = ex.link_to_budget
    JOIN event AS ev ON ev.event_id = b.link_to_event
    WHERE ev.event_name = 'October Meeting'
      AND ex.approved = 'true'

    UNION ALL

    SELECT expense_id, category, cost,
           TRIM(SUBSTR(rest, 1, INSTR(rest, ',') - 1)),
           SUBSTR(rest, INSTR(rest, ',') + 1)
    FROM purchase_items
    WHERE rest <> ''
), distinct_purchase_items AS (
    SELECT DISTINCT expense_id, category, cost, item_name
    FROM purchase_items
    WHERE item_name <> ''
)
SELECT item_name, category, SUM(cost) AS total_cost
FROM distinct_purchase_items
GROUP BY category, item_name
ORDER BY category, item_name;
