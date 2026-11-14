WITH normalized_prices AS (
  SELECT
    date,
    ticker,
    -- Normalize volume: handle "K", "M", "-", and cast to number
    CASE
      WHEN volume = '-' THEN 0
      WHEN volume LIKE '%K' THEN CAST(REPLACE(volume, 'K', '') AS FLOAT) * 1000
      WHEN volume LIKE '%M' THEN CAST(REPLACE(volume, 'M', '') AS FLOAT) * 1000000
      ELSE CAST(volume AS FLOAT)
    END AS norm_volume
  FROM bitcoin_prices
  WHERE date BETWEEN '2021-08-01' AND '2021-08-10'
),
ranked_volumes AS (
  SELECT
    date,
    ticker,
    norm_volume,
    -- Rank rows within each ticker by date, but only for rows with nonzero volume
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS overall_rank,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) FILTER (WHERE norm_volume > 0) AS nonzero_rank
  FROM normalized_prices
),
previous_nonzero_volumes AS (
  SELECT
    r1.date,
    r1.ticker,
    r1.norm_volume,
    (
      SELECT r2.norm_volume
      FROM ranked_volumes r2
      WHERE r2.ticker = r1.ticker
        AND r2.date < r1.date
        AND r2.norm_volume > 0
      ORDER BY r2.date DESC
      LIMIT 1
    ) AS prev_nonzero_volume
  FROM ranked_volumes r1
)
SELECT
  date,
  ticker,
  norm_volume AS volume,
  prev_nonzero_volume,
  CASE
    WHEN prev_nonzero_volume IS NOT NULL AND prev_nonzero_volume != 0
      THEN ((norm_volume - prev_nonzero_volume) / prev_nonzero_volume) * 100
    ELSE NULL
  END AS pct_change_vs_prev_nonzero
FROM previous_nonzero_volumes
ORDER BY ticker, date;
