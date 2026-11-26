SELECT district.A2 AS district_name
FROM client
JOIN disp ON disp.client_id = client.client_id
JOIN account ON account.account_id = disp.account_id
JOIN district ON district.district_id = account.district_id
WHERE client.gender = 'F'
  AND client.birth_date = '1976-01-29'
  AND disp.type = 'OWNER'
LIMIT 1000
