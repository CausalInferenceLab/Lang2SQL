SELECT 100.0 * (closing.balance - opening.balance) / NULLIF(opening.balance, 0) AS percentage_increase
FROM main.loan AS l
JOIN main.trans AS opening
  ON opening.account_id = l.account_id AND opening.date = '1993-03-22'
JOIN main.trans AS closing
  ON closing.account_id = l.account_id AND closing.date = '1998-12-27'
WHERE l.date = '1993-07-05'
LIMIT 1000
