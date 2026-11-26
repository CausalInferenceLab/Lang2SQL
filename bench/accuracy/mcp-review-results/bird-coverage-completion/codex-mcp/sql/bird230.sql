WITH regional_loans AS (
    SELECT l.loan_id, l.account_id, l.amount, l.duration,
           l.payments * l.duration AS total_payments, l.status,
           d.A2 AS district, d.A3 AS region,
           COUNT(*) OVER (PARTITION BY d.A3) AS loans_in_region
    FROM main.loan AS l
    JOIN main.account AS a ON a.account_id = l.account_id
    JOIN main.district AS d ON d.district_id = a.district_id
),
ranked_loans AS (
    SELECT l.*, c.client_id, c.birth_date,
           DENSE_RANK() OVER (ORDER BY l.amount DESC) AS amount_rank
    FROM regional_loans AS l
    JOIN main.disp AS h ON h.account_id = l.account_id AND h.type = 'OWNER'
    JOIN main.client AS c ON c.client_id = h.client_id
    WHERE c.gender = 'F'
),
top_loans AS (
    SELECT *
    FROM ranked_loans
    WHERE amount_rank <= 3
),
account_activity AS (
    SELECT t.account_id, COUNT(*) AS transaction_count,
           SUM(CASE WHEN t.type = 'PRIJEM' THEN t.amount ELSE 0 END) AS deposits
    FROM main.trans AS t
    WHERE t.account_id IN (SELECT account_id FROM top_loans)
    GROUP BY t.account_id
),
regional_salaries AS (
    SELECT A3 AS region, ROUND(AVG(A11), 2) AS region_average_salary
    FROM main.district
    GROUP BY A3
)
SELECT l.client_id,
       ROUND(l.amount, 2) AS loan_amount,
       l.duration AS duration_months,
       ROUND(l.total_payments, 2) AS total_payments,
       ROUND(l.total_payments - l.amount, 2) AS interest_paid,
       l.status AS loan_status,
       l.district,
       l.region,
       COALESCE(a.transaction_count, 0) AS transaction_count,
       ROUND(COALESCE(a.deposits, 0), 2) AS total_deposits,
       CASE
           WHEN a.deposits > 2 * l.amount THEN 'High Income'
           WHEN a.deposits > l.amount THEN 'Medium Income'
           ELSE 'Low Income'
       END AS income_category,
       l.loans_in_region,
       s.region_average_salary,
       2026 - CAST(strftime('%Y', l.birth_date) AS INTEGER)
            - (strftime('%m-%d', l.birth_date) > '10-09') AS completed_age
FROM top_loans AS l
LEFT JOIN account_activity AS a ON a.account_id = l.account_id
JOIN regional_salaries AS s ON s.region = l.region
ORDER BY l.amount DESC, l.loan_id
LIMIT -1
