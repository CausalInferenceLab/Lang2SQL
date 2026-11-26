WITH annual_prices AS (
    SELECT
        CAST(substr(t.txn_date, -4) AS INTEGER) AS year,
        m.region,
        SUM(t.quantity * p.price) / NULLIF(SUM(t.quantity), 0) AS avg_usd_per_btc
    FROM main.bitcoin_transactions AS t
    JOIN main.bitcoin_members AS m ON m.member_id = t.member_id
    JOIN main.bitcoin_prices AS p
      ON p.ticker = t.ticker AND p.market_date = t.txn_date
    WHERE t.ticker = 'BTC' AND t.txn_type = 'BUY'
    GROUP BY year, m.region
),
price_history AS (
    SELECT
        year,
        region,
        avg_usd_per_btc,
        LAG(year) OVER region_years AS prior_year,
        LAG(avg_usd_per_btc) OVER region_years AS prior_price
    FROM annual_prices
    WINDOW region_years AS (PARTITION BY region ORDER BY year)
)
SELECT
    year,
    region,
    avg_usd_per_btc,
    RANK() OVER (PARTITION BY year ORDER BY avg_usd_per_btc DESC) AS price_rank,
    CASE WHEN prior_year = year - 1 THEN
        100.0 * (avg_usd_per_btc - prior_price) / NULLIF(prior_price, 0)
    END AS yoy_cost_change_pct
FROM price_history
WHERE prior_year IS NOT NULL
ORDER BY year, price_rank, region
LIMIT 1000
