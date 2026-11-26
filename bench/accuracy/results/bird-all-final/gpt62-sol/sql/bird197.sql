SELECT 100.0 * (ending.balance - starting.balance) / NULLIF(starting.balance, 0) AS percentage_increase
FROM trans AS starting
JOIN trans AS ending ON ending.account_id = starting.account_id
WHERE starting.date = '1993-03-22'
  AND ending.date = '1998-12-27'
  AND EXISTS (
    SELECT 1
    FROM loan AS l
    WHERE l.account_id = starting.account_id
      AND l.date = '1993-07-05'
  );
