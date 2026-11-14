WITH txn AS (
  SELECT
    customer_id,
    DATE(transaction_date) AS txn_date,
    -- Assume 'amount' column; deposits positive, other types negative
    CASE 
      WHEN transaction_type = 'deposit' THEN amount
      ELSE -amount
    END AS txn_amount
  FROM customer_transactions
),
days AS (
  -- Generate all days per customer covering min to max txn_date
  SELECT
    customer_id,
    d AS day
  FROM (
    SELECT
      customer_id,
      MIN(txn_date) AS min_date,
      MAX(txn_date) AS max_date
    FROM txn
    GROUP BY customer_id
  ) t
  JOIN (
    SELECT 
      DATE(MIN(transaction_date), '+' || n || ' day') AS d
    FROM customer_transactions, 
      (WITH RECURSIVE nums(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM nums WHERE n < 999) SELECT n FROM nums)
    GROUP BY n
  ) ddt
    ON ddt.d BETWEEN t.min_date AND t.max_date
),
daily_sums AS (
  -- Sum transactions per customer per day (may be zero if no tx)
  SELECT
    days.customer_id,
    days.day,
    COALESCE(SUM(txn.txn_amount), 0) AS day_amount
  FROM days
    LEFT JOIN txn ON txn.customer_id = days.customer_id AND txn.txn_date = days.day
  GROUP BY days.customer_id, days.day
),
running_balance AS (
  SELECT
    ds.customer_id,
    ds.day,
    -- Cumulative sum of day_amount up to and including this day
    (SELECT SUM(ds2.day_amount)
     FROM daily_sums ds2
     WHERE ds2.customer_id = ds.customer_id AND ds2.day <= ds.day
    ) AS balance
  FROM daily_sums ds
),
rolling_avg_balance AS (
  SELECT
    rb.customer_id,
    rb.day,
    -- Only compute if at least 30 days of balance, else NULL
    CASE
      WHEN (
        SELECT COUNT(*) 
        FROM running_balance rbi
        WHERE rbi.customer_id = rb.customer_id AND rbi.day <= rb.day AND rbi.day > DATE(rb.day, '-30 day')
      ) = 30
      THEN 
        MAX(0, (
          SELECT AVG(balance*1.0)
          FROM running_balance rbi
          WHERE rbi.customer_id = rb.customer_id AND rbi.day <= rb.day AND rbi.day > DATE(rb.day, '-30 day')
        ))
      ELSE NULL
    END AS rolling_30_avg
  FROM running_balance rb
),
monthly_max_rolling_avg AS (
  SELECT
    customer_id,
    strftime('%Y-%m', day) AS month,
    MAX(rolling_30_avg) AS max_rolling_avg
  FROM rolling_avg_balance
  WHERE rolling_30_avg IS NOT NULL
  GROUP BY customer_id, month
),
first_months AS (
  -- Baseline period: first available month per customer
  SELECT
    customer_id,
    MIN(month) AS first_month
  FROM monthly_max_rolling_avg
  GROUP BY customer_id
),
valid_months AS (
  -- Exclude each customer's first month
  SELECT
    m.customer_id,
    m.month,
    m.max_rolling_avg
  FROM monthly_max_rolling_avg m
    JOIN first_months f ON m.customer_id = f.customer_id
  WHERE m.month > f.first_month
)
SELECT
  month,
  SUM(max_rolling_avg) AS total_max_30day_avg_balance
FROM valid_months
GROUP BY month
ORDER BY month;
