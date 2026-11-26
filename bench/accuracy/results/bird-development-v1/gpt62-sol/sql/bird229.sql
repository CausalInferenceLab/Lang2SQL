SELECT SUM(CASE t.k_symbol
             WHEN 'UROK' THEN t.amount
             WHEN 'SANKC. UROK' THEN -t.amount
           END) AS net_interest_czk
FROM trans AS t
WHERE t.date >= '1998-01-01'
  AND t.date < '1999-01-01'
  AND t.k_symbol IN ('UROK', 'SANKC. UROK')
  AND EXISTS (
    SELECT 1
    FROM disp AS d
    WHERE d.client_id = 2823
      AND d.account_id = t.account_id
  );
