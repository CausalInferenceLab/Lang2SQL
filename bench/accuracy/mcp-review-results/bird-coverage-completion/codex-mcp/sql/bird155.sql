WITH qualifying_transactions AS (
    SELECT unixepoch(t.Date || ' ' || t.Time) AS transaction_seconds
    FROM main.transactions_1k AS t
    JOIN main.gasstations AS g ON g.GasStationID = t.GasStationID
    WHERE g.Country = 'CZE' AND t.Price > 1000
)
SELECT (MAX(transaction_seconds) - MIN(transaction_seconds)) / 60 AS elapsed_minutes
FROM qualifying_transactions
LIMIT 1
