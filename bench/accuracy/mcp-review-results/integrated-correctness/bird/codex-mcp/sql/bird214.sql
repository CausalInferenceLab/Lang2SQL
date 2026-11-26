WITH recipient_totals AS (
    SELECT t.bank AS recipient_bank_code,
           t.account AS recipient_counter_account,
           COUNT(*) AS outgoing_payment_count,
           SUM(t.amount) AS total_paid
    FROM main.trans AS t
    JOIN main.account AS a ON a.account_id = t.account_id
    JOIN main.district AS d ON d.district_id = a.district_id
    WHERE t.date >= '1998-01-01'
      AND t.date < '1999-01-01'
      AND t.type = 'VYDAJ'
      AND t.bank IS NOT NULL
      AND t.bank <> ''
      AND t.account IS NOT NULL
      AND CAST(d.A4 AS INTEGER) >= 250000
      AND a.account_id IN (
          SELECT p.account_id
          FROM main.disp AS p
          JOIN main.card AS c ON c.disp_id = p.disp_id
          WHERE p.type = 'OWNER'
            AND c.type = 'gold'
      )
    GROUP BY t.bank, t.account
)
SELECT recipient_bank_code,
       recipient_counter_account,
       outgoing_payment_count,
       total_paid
FROM recipient_totals
WHERE total_paid = (SELECT MAX(total_paid) FROM recipient_totals)
ORDER BY recipient_bank_code, recipient_counter_account
LIMIT 1000
