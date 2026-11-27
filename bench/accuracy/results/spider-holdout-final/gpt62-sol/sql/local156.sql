WITH annual_prices AS (
    SELECT
        CAST(substr(t.txn_date, -4) AS INTEGER) AS purchase_year,
        m.region,
        SUM(t.quantity * p.price) / NULLIF(SUM(t.quantity), 0) AS avg_purchase_price
    FROM bitcoin_transactions AS t
    JOIN bitcoin_members AS m ON m.member_id = t.member_id
    JOIN bitcoin_prices AS p ON p.ticker = t.ticker AND p.market_date = t.txn_date
    WHERE t.ticker = 'BTC' AND t.txn_type = 'BUY'
    GROUP BY CAST(substr(t.txn_date, -4) AS INTEGER), m.region
), regional_history AS (
    SELECT
        purchase_year,
        region,
        avg_purchase_price,
        LAG(purchase_year) OVER regional_years AS previous_year,
        LAG(avg_purchase_price) OVER regional_years AS previous_avg_price
    FROM annual_prices
    WINDOW regional_years AS (PARTITION BY region ORDER BY purchase_year)
)
SELECT
    purchase_year,
    region,
    avg_purchase_price,
    RANK() OVER (PARTITION BY purchase_year ORDER BY avg_purchase_price DESC) AS price_rank,
    CASE WHEN previous_year = purchase_year - 1 THEN
        100.0 * (avg_purchase_price - previous_avg_price) / NULLIF(previous_avg_price, 0)
    END AS annual_pct_change
FROM regional_history
WHERE previous_year IS NOT NULL
ORDER BY purchase_year, price_rank, region;
