SELECT a.account_id, a.frequency
FROM account AS a
JOIN district AS d ON d.district_id = a.district_id
WHERE d.A3 = 'east Bohemia'
  AND a.date >= '1995-01-01'
  AND a.date < '2001-01-01';
