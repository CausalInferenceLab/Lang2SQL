SELECT disp.type, COUNT(DISTINCT disp.account_id) AS account_count
FROM disp
JOIN account ON account.account_id = disp.account_id
JOIN district ON district.district_id = account.district_id
WHERE disp.type <> 'OWNER'
  AND district.A11 > 8000
  AND district.A11 <= 9000
GROUP BY disp.type
LIMIT 1000
