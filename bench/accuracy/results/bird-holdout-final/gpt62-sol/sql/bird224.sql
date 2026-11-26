SELECT
    d.A2 AS district,
    l.amount AS approved_amount,
    l.date AS approval_date,
    (
        SELECT t.balance
        FROM trans AS t
        WHERE t.account_id = l.account_id
          AND t.date <= l.date
        ORDER BY t.date DESC, t.trans_id DESC
        LIMIT 1
    ) AS end_of_day_balance
FROM loan AS l
JOIN account AS a ON a.account_id = l.account_id
JOIN district AS d ON d.district_id = a.district_id
WHERE l.duration = 24
  AND a.frequency = 'POPLATEK TYDNE'
ORDER BY l.amount DESC, l.date ASC, l.loan_id ASC
LIMIT 10;
