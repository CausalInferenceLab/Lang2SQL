SELECT notes
FROM main.income
WHERE source = 'Fundraising'
  AND date_received = '2019-09-14'
LIMIT 1000
