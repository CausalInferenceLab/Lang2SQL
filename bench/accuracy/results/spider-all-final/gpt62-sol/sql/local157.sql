WITH parsed AS (
    SELECT ticker,
           SUBSTR(market_date, 7, 4) || '-' || SUBSTR(market_date, 4, 2) || '-' || SUBSTR(market_date, 1, 2) AS market_date,
           UPPER(TRIM(volume)) AS volume_text
    FROM bitcoin_prices
), normalized AS (
    SELECT ticker, market_date,
           CASE
               WHEN volume_text = '-' THEN 0.0
               WHEN SUBSTR(volume_text, -1) = 'K'
                   THEN CAST(SUBSTR(volume_text, 1, LENGTH(volume_text) - 1) AS REAL) * 1000.0
               WHEN SUBSTR(volume_text, -1) = 'M'
                   THEN CAST(SUBSTR(volume_text, 1, LENGTH(volume_text) - 1) AS REAL) * 1000000.0
               ELSE CAST(volume_text AS REAL)
           END AS trading_volume
    FROM parsed
    WHERE market_date < '2021-08-11'
), previous_dates AS (
    SELECT ticker, market_date, trading_volume,
           MAX(CASE WHEN trading_volume <> 0 THEN market_date END) OVER (
               PARTITION BY ticker ORDER BY market_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS previous_nonzero_date
    FROM normalized
)
SELECT d.ticker,
       d.market_date,
       100.0 * (d.trading_volume - p.trading_volume) / p.trading_volume AS daily_volume_pct_change
FROM previous_dates AS d
LEFT JOIN normalized AS p
    ON p.ticker = d.ticker AND p.market_date = d.previous_nonzero_date
WHERE d.market_date >= '2021-08-01' AND d.market_date < '2021-08-11'
ORDER BY d.ticker, d.market_date;
