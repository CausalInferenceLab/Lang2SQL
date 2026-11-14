WITH regional_txns AS (
    SELECT
        bm.region,
        STRFTIME('%Y', bt.txn_date) AS year,
        SUM(bt.quantity * bp.price) AS total_dollars,
        SUM(bt.quantity) AS total_quantity
    FROM bitcoin_transactions bt
    JOIN bitcoin_members bm ON bt.member_id = bm.member_id
    JOIN bitcoin_prices bp ON bt.txn_date = bp.market_date
    WHERE bt.txn_type = 'BUY' AND bp.ticker = bt.ticker
    GROUP BY bm.region, year
),
filtered_data AS (
    SELECT *
    FROM regional_txns r1
    WHERE year > (
        SELECT MIN(STRFTIME('%Y', txn_date))
        FROM bitcoin_transactions bt2
        JOIN bitcoin_members bm2 ON bt2.member_id = bm2.member_id
        WHERE bm2.region = r1.region
    )
),
avg_price AS (
    SELECT
        region,
        year,
        total_dollars / total_quantity AS avg_purchase_price
    FROM filtered_data
),
ranked AS (
    SELECT
        region,
        year,
        avg_purchase_price,
        RANK() OVER (PARTITION BY year ORDER BY avg_purchase_price DESC) AS price_rank
    FROM avg_price
),
pct_change AS (
    SELECT
        region,
        year,
        avg_purchase_price,
        price_rank,
        ROUND(
            100.0 * (avg_purchase_price - LAG(avg_purchase_price) OVER (PARTITION BY region ORDER BY year)) / LAG(avg_purchase_price) OVER (PARTITION BY region ORDER BY year),
            2
        ) AS pct_change_from_prev_year
    FROM ranked
)
SELECT *
FROM pct_change
ORDER BY year, price_rank;
