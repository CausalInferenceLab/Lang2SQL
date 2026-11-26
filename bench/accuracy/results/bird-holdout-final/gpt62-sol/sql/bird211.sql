SELECT
    COUNT(CASE WHEN TRIM(t.k_symbol) = 'SIPO' THEN 1 END) AS household_payments,
    COUNT(CASE WHEN NULLIF(TRIM(t.k_symbol), '') <> 'SIPO' THEN 1 END) AS other_purpose_payments
FROM trans AS t
WHERE t.amount > 4000
  AND t.type = 'VYDAJ'
  AND EXISTS (
      SELECT 1
      FROM disp AS d
      JOIN client AS c ON c.client_id = d.client_id
      WHERE d.account_id = t.account_id
        AND c.gender = 'M'
        AND c.birth_date >= '1974-01-01'
        AND c.birth_date < '1977-01-01'
  );
