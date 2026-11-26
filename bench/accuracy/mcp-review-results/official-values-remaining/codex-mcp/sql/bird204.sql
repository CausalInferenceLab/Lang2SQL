SELECT COUNT(*) AS transaction_count
FROM main.trans AS t
WHERE (t.k_symbol IS NULL OR TRIM(t.k_symbol) = '')
  AND t.account_id IN (
      SELECT d.account_id
      FROM main.disp AS d
      JOIN main.client AS c ON c.client_id = d.client_id
      WHERE d.type = 'OWNER'
        AND c.gender = 'M'
        AND c.birth_date >= '1974-01-01'
        AND c.birth_date < '1977-01-01'
  )
LIMIT 1
