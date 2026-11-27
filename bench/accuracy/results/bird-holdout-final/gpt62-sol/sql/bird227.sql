SELECT
    t.bank AS bank_code,
    t.account AS account_number,
    COUNT(*) AS outgoing_payment_count,
    SUM(t.amount) AS total_amount_paid
FROM trans AS t
WHERE t.type = 'VYDAJ'
  AND t.date < '1996-01-03'
  AND t.bank IS NOT NULL
  AND t.account IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM loan AS l
      WHERE l.account_id = t.account_id
        AND l.amount = 98832
        AND l.date = '1996-01-03'
  )
GROUP BY t.bank, t.account
ORDER BY total_amount_paid DESC;
