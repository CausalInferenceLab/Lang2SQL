SELECT d.type,
       COUNT(DISTINCT a.account_id) AS distinct_account_count
FROM disp AS d
JOIN account AS a ON a.account_id = d.account_id
JOIN district AS r ON r.district_id = a.district_id
WHERE d.type <> 'OWNER'
  AND r.A11 > 8000
  AND r.A11 <= 9000
GROUP BY d.type;
