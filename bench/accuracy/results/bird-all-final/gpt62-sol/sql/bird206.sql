WITH regional_totals AS (
    SELECT d.A3 AS region, SUM(l.amount) AS total_approved_amount
    FROM loan AS l
    JOIN account AS a ON a.account_id = l.account_id
    JOIN district AS d ON d.district_id = a.district_id
    WHERE DATE(l.date) <= '1995-12-31'
      AND DATE(l.date, '+' || l.duration || ' months') >= '1995-12-31'
    GROUP BY d.A3
)
SELECT region, total_approved_amount
FROM regional_totals
WHERE total_approved_amount = (SELECT MAX(total_approved_amount) FROM regional_totals)
ORDER BY region;
