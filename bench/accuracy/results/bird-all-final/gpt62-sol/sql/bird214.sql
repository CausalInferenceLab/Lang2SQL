SELECT
    t.bank AS recipient_bank_code,
    t.account AS recipient_counter_account,
    COUNT(*) AS outgoing_payment_count,
    SUM(t.amount) AS total_amount_paid
FROM trans AS t
JOIN account AS a ON a.account_id = t.account_id
JOIN district AS d ON d.district_id = a.district_id
WHERE t.type = 'VYDAJ'
  AND t.date >= '1998-01-01'
  AND t.date < '1999-01-01'
  AND CAST(d.A4 AS INTEGER) >= 250000
  AND t.bank IS NOT NULL
  AND t.account IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM disp AS dp
      JOIN card AS c ON c.disp_id = dp.disp_id
      WHERE dp.account_id = a.account_id
        AND dp.type = 'OWNER'
        AND c.type = 'gold'
  )
GROUP BY t.bank, t.account
ORDER BY total_amount_paid DESC, recipient_bank_code, recipient_counter_account;
