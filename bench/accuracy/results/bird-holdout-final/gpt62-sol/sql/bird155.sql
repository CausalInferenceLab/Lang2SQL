WITH qualifying_transactions AS (
    SELECT unixepoch(t.Date || ' ' || t.Time) AS transaction_time
    FROM transactions_1k AS t
    JOIN gasstations AS g ON g.GasStationID = t.GasStationID
    WHERE g.Country = 'CZE'
      AND t.Price > 1000
)
SELECT (MAX(transaction_time) - MIN(transaction_time)) / 60 AS elapsed_minutes
FROM qualifying_transactions;
