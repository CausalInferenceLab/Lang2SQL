WITH loans_by_account AS (
    SELECT account_id, SUM(amount) AS amount_lent
    FROM loan
    GROUP BY account_id
), district_year AS (
    SELECT
        STRFTIME('%Y', a.date) AS opening_year,
        a.district_id,
        COUNT(*) AS accounts_opened,
        SUM(COALESCE(l.amount_lent, 0)) AS amount_lent
    FROM account AS a
    JOIN district AS d ON d.district_id = a.district_id
    LEFT JOIN loans_by_account AS l ON l.account_id = a.account_id
    WHERE d.A3 = 'central Bohemia'
      AND a.date >= '1996-01-01'
    GROUP BY STRFTIME('%Y', a.date), a.district_id
)
SELECT
    opening_year,
    SUM(accounts_opened) AS accounts_opened,
    SUM(amount_lent) AS total_amount_lent,
    ROUND(AVG(CAST(amount_lent AS REAL) / accounts_opened), 2)
        AS avg_district_amount_lent_per_account
FROM district_year
GROUP BY opening_year
ORDER BY opening_year;
