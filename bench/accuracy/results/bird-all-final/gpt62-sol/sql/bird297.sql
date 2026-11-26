SELECT notes
FROM income
WHERE date(date_received) = '2019-09-14'
  AND source LIKE '%fundrais%';
