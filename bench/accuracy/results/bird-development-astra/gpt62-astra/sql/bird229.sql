SELECT SUM(CASE t.k_symbol
               WHEN 'UROK' THEN t.amount
               WHEN 'SANKC. UROK' THEN -t.amount
           END) AS net_interest_czk
FROM trans AS t
WHERE t.account_id IN (
    SELECT d.account_id
    FROM disp AS d
    WHERE d.client_id = 2823
)
  AND t.date >= '1998-01-01'
  AND t.date < '1999-01-01'
  AND t.k_symbol IN ('UROK', 'SANKC. UROK');
