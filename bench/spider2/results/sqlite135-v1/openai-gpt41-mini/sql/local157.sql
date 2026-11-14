WITH volume_converted AS (
    SELECT
        ticker,
        date,
        CASE
            WHEN volume LIKE '%M' THEN CAST(REPLACE(volume, 'M', '') AS FLOAT) * 1000000
            WHEN volume LIKE '%K' THEN CAST(REPLACE(volume, 'K', '') AS FLOAT) * 1000
            WHEN volume = '-' THEN 0
            ELSE CAST(volume AS FLOAT)
        END AS volume_val
    FROM bitcoin_prices
    WHERE date BETWEEN '2021-08-01' AND '2021-08-10'
),
prev_volume AS (
    SELECT
        v1.ticker,
        v1.date,
        v1.volume_val,
        (
            SELECT MAX(v2.volume_val)
            FROM volume_converted v2
            WHERE v2.ticker = v1.ticker
              AND v2.date < v1.date
              AND v2.volume_val > 0
        ) AS prev_vol
    FROM volume_converted v1
)
SELECT
    ticker,
    date,
    ROUND(
      CASE 
        WHEN prev_vol IS NULL THEN NULL
        ELSE (volume_val - prev_vol) / prev_vol * 100
      END, 6
    ) AS daily_percentage_change_volume
FROM prev_volume
ORDER BY ticker, date;
