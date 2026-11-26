SELECT COUNT(*) AS account_count
FROM account AS a
JOIN district AS d ON d.district_id = a.district_id
WHERE d.A3 = 'north Bohemia'
  AND a.date <= '1995-12-31'
  AND a.account_id IN (
      SELECT t.account_id
      FROM trans AS t
      WHERE t.bank = 'AB'
  )
LIMIT 1
