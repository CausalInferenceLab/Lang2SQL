SELECT d.A2 AS branch_name, COUNT(t.trans_id) AS transaction_count
FROM district AS d
LEFT JOIN account AS a ON a.district_id = d.district_id
LEFT JOIN trans AS t ON t.account_id = a.account_id
  AND t.date <= '1998-10-14'
  AND t.k_symbol IS NULL
WHERE d.A3 = 'north Moravia'
GROUP BY d.district_id, d.A2
ORDER BY transaction_count DESC, branch_name ASC
LIMIT 3;
