SELECT a.frequency AS statement_frequency,
       o.k_symbol AS payment_purpose_code
FROM account AS a
JOIN "order" AS o ON o.account_id = a.account_id
WHERE a.account_id = 3
  AND o.amount = 3539;
