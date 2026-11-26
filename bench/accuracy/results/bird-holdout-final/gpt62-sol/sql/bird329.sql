WITH RECURSIVE expense_items(expense_id, item_name, remaining) AS (
    SELECT e.expense_id, '',
           CASE WHEN e.expense_description LIKE 'Bakery - %'
                THEN substr(e.expense_description, length('Bakery - ') + 1)
                ELSE e.expense_description
           END || ','
    FROM expense AS e
    JOIN member AS m ON m.member_id = e.link_to_member
    WHERE m.first_name = 'Sacha' AND m.last_name = 'Harrison'

    UNION ALL

    SELECT expense_id,
           trim(substr(remaining, 1, instr(remaining, ',') - 1)),
           substr(remaining, instr(remaining, ',') + 1)
    FROM expense_items
    WHERE remaining <> ''
)
SELECT item_name, COUNT(DISTINCT expense_id) AS purchase_count
FROM expense_items
WHERE item_name <> ''
GROUP BY item_name
ORDER BY item_name COLLATE NOCASE;
